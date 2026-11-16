import json
import queue
import threading
import unittest
from unittest.mock import Mock, patch

import delivery
from forwarder_status import StatusRegistry, healthy
import status_reporter


class StatusReporterTest(unittest.TestCase):
    def setUp(self):
        self.registry = StatusRegistry()
        for name in ('logs', 'log_sender', 'metrics', 'health'):
            self.registry.update(name, enabled=True, state='running')
            self.registry.event(name, 'poll')
        self.work = queue.Queue(maxsize=300)
        self.patches = [patch.object(status_reporter, 'STATUS', self.registry),
                        patch.object(delivery, 'STATUS', self.registry),
                        patch.object(status_reporter, 'send_q', self.work),
                        patch.object(status_reporter, 'INGEST_URL', 'http://example.invalid/status'),
                        patch.object(status_reporter, 'TOKEN', 'secret-test-token')]
        for item in self.patches:
            item.start()
            self.addCleanup(item.stop)

    def test_실제전송본문은_상태와_큐크기를_담고_토큰과_주소를_담지않는다(self):
        self.work.put([{}, {}])
        self.registry.event('logs', 'collected', 2)
        with patch.object(delivery.requests, 'post', return_value=Mock(status_code=200)) as post:
            self.assertTrue(status_reporter.report_once(1).success)
        body = json.loads(post.call_args.kwargs['data'])
        self.assertEqual(body['log_queue_size'], 1)
        self.assertEqual(body['log_queue_capacity'], 300)
        self.assertEqual(body['streams']['logs']['collected_items'], 2)
        self.assertEqual(body['schema_version'], 1)
        self.assertEqual(body['sequence'], 1)
        self.assertEqual(post.call_args.kwargs['headers']['X-MCP-TOKEN'], 'secret-test-token')
        self.assertNotIn('secret-test-token', json.dumps(body))
        self.assertNotIn('http://', json.dumps(body))

    def test_보고실패를_수집기상태에_재귀적으로_추가하지않는다(self):
        before = self.registry.snapshot()['streams']
        with patch.object(delivery.requests, 'post', return_value=Mock(status_code=500)) as post:
            self.assertFalse(status_reporter.report_once(1).success)
        self.assertEqual(post.call_count, 1)
        self.assertEqual(self.registry.snapshot()['streams'], before)
        self.assertTrue(healthy(self.registry.snapshot()))

    def test_실패뒤_옛보고를_재전송하지않고_최신카운터와_새순번을_보낸다(self):
        with patch.object(delivery.requests, 'post', side_effect=[Mock(status_code=500), Mock(status_code=200)]) as post:
            status_reporter.report_once(1)
            self.registry.event('logs', 'dropped', 10, 'queue_full')
            status_reporter.report_once(2)
        first, second = [json.loads(call.kwargs['data']) for call in post.call_args_list]
        self.assertEqual(first['run_id'], second['run_id'])
        self.assertEqual(first['started_at'], second['started_at'])
        self.assertEqual((first['sequence'], second['sequence']), (1, 2))
        self.assertEqual(first['streams']['logs']['dropped_items'], 0)
        self.assertEqual(second['streams']['logs']['dropped_items'], 10)
        self.assertTrue(self.work.empty())

    def test_큐가가득차도_상태는_큐에_넣지않고_독립적으로_전송한다(self):
        for _ in range(300):
            self.work.put([{}])
        with patch.object(delivery.requests, 'post', return_value=Mock(status_code=200)):
            self.assertTrue(status_reporter.report_once(1).success)
        self.assertEqual(self.work.qsize(), 300)

    def test_보고루프는_순번을_증가시키고_종료요청에_반응한다(self):
        stop = threading.Event()
        calls = []
        def report(sequence):
            calls.append(sequence)
            if sequence == 2:
                stop.set()
        with patch.object(status_reporter, 'report_once', side_effect=report), \
             patch.object(status_reporter, 'REPORT_INTERVAL', 0):
            status_reporter.main(stop)
        self.assertEqual(calls, [1, 2])
