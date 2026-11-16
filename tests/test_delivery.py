import contextlib
import io
import unittest
from unittest.mock import Mock, patch

import requests
import delivery
import forwarder_status
import health_forwarder
import metric_forwarder


def response(code):
    return Mock(status_code=code)


class DeliveryTest(unittest.TestCase):
    def setUp(self):
        self.registry = forwarder_status.StatusRegistry()
        self.patches = [patch.object(delivery, 'STATUS', self.registry),
                        patch.object(metric_forwarder, 'STATUS', self.registry),
                        patch.object(health_forwarder, 'STATUS', self.registry),
                        patch.object(metric_forwarder, 'MCP_METRIC_INGEST_URL', 'http://example.invalid'),
                        patch.object(health_forwarder, 'MCP_HEALTH_INGEST_URL', 'http://example.invalid'),
                        patch.object(metric_forwarder, 'HEADERS', {'X-MCP-TOKEN': 'test'}),
                        patch.object(health_forwarder, 'HEADERS', {'X-MCP-TOKEN': 'test'}),
                        patch.object(delivery.time, 'sleep')]
        for item in self.patches:
            item.start()
            self.addCleanup(item.stop)

    def post(self, **kwargs):
        return delivery.post_json('metrics', 'http://example.invalid', {'X-MCP-TOKEN': 'test'},
                                  {'eventId': 'same-id'}, **kwargs)

    def state(self):
        return self.registry.snapshot()['streams']['metrics']

    def test_지표_401응답을_성공으로_기록하지않는다(self):
        output = io.StringIO()
        with patch.object(delivery.requests, 'post', return_value=response(401)) as post, contextlib.redirect_stdout(output):
            result = metric_forwarder.send_metric(0.1, 100, 200)
        self.assertIs(result, False)
        self.assertNotIn('전송 완료', output.getvalue())
        self.assertEqual(post.call_count, 1)
        self.assertEqual(self.state()['last_send_error'], 'authentication_failed')
        self.assertEqual(self.state()['sent_items'], 0)
        self.assertEqual(self.state()['dropped_items'], 1)

    def test_Health_401응답을_실패로_반환한다(self):
        with patch.object(delivery.requests, 'post', return_value=response(401)) as post:
            self.assertIs(health_forwarder.send_health({'ts': 1, 'status': 'UP'}), False)
        self.assertEqual(post.call_count, 1)
        state = self.registry.snapshot()['streams']['health']
        self.assertEqual(state['sent_items'], 0)
        self.assertEqual(state['last_send_error'], 'authentication_failed')

    def test_JSON본문의_형식헤더를_추가하고_호출자헤더는_변경하지않는다(self):
        headers = {'X-MCP-TOKEN': 'test'}
        with patch.object(delivery.requests, 'post', return_value=response(200)) as post:
            delivery.post_json('metrics', 'http://example.invalid', headers, {'value': 1})
        self.assertEqual(post.call_args.kwargs['headers']['Content-Type'], 'application/json')
        self.assertEqual(post.call_args.kwargs['headers']['X-MCP-TOKEN'], 'test')
        self.assertEqual(headers, {'X-MCP-TOKEN': 'test'})

    def test_성공응답만_전송건수에_반영한다(self):
        with patch.object(delivery.requests, 'post', return_value=response(204)):
            self.assertTrue(self.post(count=5).success)
        self.assertEqual(self.state()['sent_items'], 5)
        self.assertIsNone(self.state()['last_send_error'])

    def test_500후_성공하면_동일페이로드로_재시도하고_오류상태를_해제한다(self):
        with patch.object(delivery.requests, 'post', side_effect=[response(500), response(200)]) as post:
            result = self.post()
        self.assertTrue(result.success)
        self.assertEqual(result.attempts, 2)
        self.assertEqual(post.call_args_list[0].kwargs['data'], post.call_args_list[1].kwargs['data'])
        self.assertEqual(self.state()['failed_attempts'], 1)
        self.assertIsNone(self.state()['last_send_error'])
        self.assertEqual(self.state()['dropped_items'], 0)

    def test_408과_429는_재시도한다(self):
        for code in (408, 429):
            with self.subTest(code=code), patch.object(delivery.requests, 'post', side_effect=[response(code), response(200)]):
                self.assertEqual(self.post().attempts, 2)

    def test_403과_400과_리다이렉트는_재시도하지않는다(self):
        for code in (403, 400, 302):
            with self.subTest(code=code), patch.object(delivery.requests, 'post', return_value=response(code)) as post:
                self.assertFalse(self.post().success)
                self.assertEqual(post.call_count, 1)
                self.assertIs(post.call_args.kwargs['allow_redirects'], False)

    def test_시간초과와_연결오류를_제한적으로_재시도한다(self):
        for error in (requests.Timeout(), requests.ConnectionError()):
            with self.subTest(error=type(error).__name__), patch.object(delivery.requests, 'post', side_effect=error) as post:
                self.assertFalse(self.post().success)
                self.assertEqual(post.call_count, 3)

    def test_재시도시간이_끝나면_추가요청을_보내지않는다(self):
        now = [0.0]
        def advance(seconds):
            now[0] += seconds
        def fail(*args, **kwargs):
            now[0] += 0.2
            raise requests.Timeout()
        with patch.object(delivery.time, 'monotonic', side_effect=lambda: now[0]), \
             patch.object(delivery.time, 'sleep', side_effect=advance), \
             patch.object(delivery.random, 'uniform', return_value=1), \
             patch.object(delivery.requests, 'post', side_effect=fail) as post:
            self.assertFalse(self.post(max_attempts=None, retry_seconds=1).success)
        self.assertEqual(post.call_count, 2)
        self.assertAlmostEqual(now[0], 1)

    def test_주소나_토큰이_없으면_설정오류로_기록한다(self):
        with patch.object(delivery.requests, 'post') as post:
            result = delivery.post_json('metrics', None, {}, {})
        self.assertEqual(result.reason, 'configuration_error')
        post.assert_not_called()

    def test_최대메모리_누락시_전송하지않고_수집실패를_기록한다(self):
        with patch.object(metric_forwarder, 'get_actuator_value', side_effect=[0.1, 100, None]), \
             patch.object(delivery.requests, 'post') as post:
            self.assertFalse(metric_forwarder.collect_once())
        post.assert_not_called()
        self.assertEqual(self.state()['collected_items'], 0)
        self.assertEqual(self.state()['last_collection_error'], 'missing_or_invalid_metric')

    def test_비정상_지표를_직접_전송해도_거절한다(self):
        for values in ((0.1, 100, None), (0.1, 100, 0), (float('nan'), 100, 200), (2, 100, 200)):
            with self.subTest(values=values), patch.object(delivery.requests, 'post') as post:
                self.assertFalse(metric_forwarder.send_metric(*values))
                post.assert_not_called()

    def test_대상서버_DOWN도_정상적으로_수집한_관측값이다(self):
        with patch.object(health_forwarder, 'check_health', return_value={'ts': 1, 'status': 'DOWN'}), \
             patch.object(delivery.requests, 'post', return_value=response(200)):
            self.assertTrue(health_forwarder.collect_once())
        state = self.registry.snapshot()['streams']['health']
        self.assertEqual(state['collected_items'], 1)
        self.assertEqual(state['sent_items'], 1)
        self.assertIsNone(state['last_collection_error'])

    def test_JVM메모리는_동일한_heap영역으로_수집한다(self):
        result = Mock(status_code=200)
        result.json.return_value = {'measurements': [{'statistic': 'VALUE', 'value': 200}]}
        with patch.object(metric_forwarder.requests, 'get', return_value=result) as get:
            self.assertEqual(metric_forwarder.get_actuator_value('jvm.memory.max'), 200)
        self.assertEqual(get.call_args.kwargs['params'], {'tag': 'area:heap'})
