import concurrent.futures
import json
import os
import queue
import tempfile
import threading
import unittest
from unittest.mock import patch

import forwarder_status as state
import log_forwarder


class StatusTest(unittest.TestCase):
    def setUp(self):
        self.now = [1000.0]
        self.registry = state.StatusRegistry(clock=lambda: self.now[0])
        self.patch = patch.object(state, 'STATUS', self.registry)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def running(self, name='metrics'):
        self.registry.update(name, enabled=True, state='running')
        self.registry.event(name, 'poll')

    def healthy(self):
        return state.healthy(self.registry.snapshot(), now=self.now[0])

    def test_스레드_예외와_정상처럼_끝난_조기종료를_실패로_기록한다(self):
        def crash():
            raise RuntimeError('secret must not be logged')
        state.run_worker('metrics', crash)
        state.run_worker('health', lambda: None)
        result = self.registry.snapshot()
        self.assertEqual(result['streams']['metrics']['state'], 'failed')
        self.assertEqual(result['streams']['health']['worker_error'], 'unexpected_return')
        self.assertNotIn('secret', json.dumps(result))
        self.assertFalse(self.healthy())

    def test_종료요청에_따른_반환은_정상종료로_구분한다(self):
        stop = threading.Event()
        stop.set()
        state.run_worker('metrics', lambda: None, stop)
        self.assertEqual(self.registry.snapshot()['streams']['metrics']['state'], 'stopped')

    def test_유휴상태여도_로그파일을_읽고있으면_수집정상이다(self):
        self.running('logs')
        self.registry.event('logs', 'collected', 0)
        self.assertTrue(self.healthy())
        self.assertEqual(self.registry.snapshot()['streams']['logs']['collected_items'], 0)

    def test_한종류라도_실패하거나_멈추면_컨테이너가_수집정상이아니다(self):
        self.running('logs')
        self.running('metrics')
        self.registry.update('metrics', state='failed')
        self.assertFalse(self.healthy())

    def test_비활성수집기는_실패로_세지않는다(self):
        self.running()
        self.registry.update('health', enabled=False, state='disabled')
        self.assertTrue(self.healthy())

    def test_모두비활성이면_정상으로_판정하지않는다(self):
        self.registry.update('health', enabled=False, state='disabled')
        self.assertFalse(self.healthy())

    def test_폴링과_상태파일이_오래되면_비정상이다(self):
        self.running()
        old = self.registry.snapshot()
        self.now[0] += 76
        self.assertFalse(state.healthy(old, now=self.now[0]))
        self.assertFalse(self.healthy())

    def test_긴수집주기의_정상대기는_스트림최신성한계까지_허용한다(self):
        self.running('health')
        self.registry.update('health', poll_stale_seconds=212)
        self.now[0] += 180
        self.assertTrue(self.healthy())
        self.now[0] += 33
        self.assertFalse(self.healthy())

    def test_수집주기와_요청예산으로_최신성한계를_계산한다(self):
        with patch.dict(os.environ, {'FORWARDER_STALE_SECONDS': '75'}):
            self.assertEqual(state.poll_stale_limit(180, collection_budget=4), 212)
            self.assertEqual(state.poll_stale_limit(180, collection_budget=12), 220)
            self.assertEqual(state.poll_stale_limit(10, collection_budget=12), 75)
        with patch.dict(os.environ, {'FORWARDER_STALE_SECONDS': '300'}):
            self.assertEqual(state.poll_stale_limit(180, collection_budget=4), 300)

    def test_긴수집주기도_오래된상태파일과_스레드실패를_정상으로_가리지않는다(self):
        self.running('health')
        self.registry.update('health', poll_stale_seconds=212)
        old = self.registry.snapshot()
        self.now[0] += 100
        self.assertFalse(state.healthy(old, now=self.now[0]))
        self.registry.update('health', state='failed')
        self.assertFalse(self.healthy())

    def test_수집실패와_전송실패는_복구후_해제된다(self):
        self.running()
        self.registry.event('metrics', 'collection_failed', reason='missing_metric')
        self.assertFalse(self.healthy())
        self.registry.event('metrics', 'collected', 1)
        self.registry.event('metrics', 'send_failed', reason='timeout')
        self.assertFalse(self.healthy())
        self.registry.event('metrics', 'sent', 1)
        self.assertTrue(self.healthy())

    def test_상태파일을_JSON으로_교체한다(self):
        self.running()
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, 'status.json')
            state.write_status(path, self.registry.snapshot())
            with open(path) as source:
                self.assertEqual(json.load(source)['streams']['metrics']['state'], 'running')
            self.assertFalse(os.path.exists(path + '.tmp'))

    def test_동시기록에서도_전송건수가_유실되지않는다(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda _: self.registry.event('metrics', 'sent', 1), range(1000)))
        self.assertEqual(self.registry.snapshot()['streams']['metrics']['sent_items'], 1000)

    def test_가장오래된_배치폐기를_로그건수로_기록한다(self):
        with patch.object(log_forwarder, 'STATUS', self.registry), \
             patch.object(log_forwarder, 'send_q', queue.Queue(maxsize=1)):
            log_forwarder.enqueue_drop_oldest([{}, {}])
            log_forwarder.enqueue_drop_oldest([{}])
            self.assertEqual(log_forwarder.send_q.get_nowait(), [{}])
        row = self.registry.snapshot()['streams']['logs']
        self.assertEqual(row['dropped_items'], 2)
        self.assertEqual(row['last_drop_reason'], 'queue_full')

    def test_로그전송스레드의_실제예외도_실패로_기록한다(self):
        work = queue.Queue()
        work.put([{}])
        with patch.object(log_forwarder, 'STATUS', self.registry), \
             patch.object(log_forwarder, 'send_q', work), \
             patch.object(log_forwarder, 'send_with_retry', side_effect=RuntimeError()):
            state.run_worker('log_sender', log_forwarder.sender_loop)
        self.assertEqual(self.registry.snapshot()['streams']['log_sender']['state'], 'failed')
        self.assertEqual(work.unfinished_tasks, 0)
