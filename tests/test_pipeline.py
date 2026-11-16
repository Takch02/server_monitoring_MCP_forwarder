import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from urllib.parse import urlsplit
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

import requests

import delivery
import status_reporter
from forwarder_status import StatusRegistry, healthy

ROOT = Path(__file__).resolve().parents[1]


class PipelineTest(unittest.TestCase):
    def setUp(self):
        self.received = []
        self.codes = []
        self.status_received = []
        self.status_codes = []
        self.metrics_failure_reported = None
        self.delay = 0
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                path = urlsplit(self.path).path
                if path == '/health':
                    code, data = 503, {'status': 'DOWN'}
                else:
                    value = 0.1 if path.endswith('cpu.usage') else 200 if path.endswith('memory.max') else 100
                    code, data = 200, {'measurements': [{'statistic': 'VALUE', 'value': value}]}
                self.reply(code, json.dumps(data).encode())

            def do_POST(self):
                body = self.rfile.read(int(self.headers['Content-Length']))
                if self.headers.get_content_type() != 'application/json':
                    self.reply(415, b'{}')
                    return
                if self.path.endswith('/forwarder-status'):
                    report = json.loads(body)
                    fixture.status_received.append(report)
                    if (fixture.metrics_failure_reported is not None
                            and report['streams']['metrics']['last_send_error'] == 'authentication_failed'):
                        fixture.metrics_failure_reported.set()
                    code = fixture.status_codes.pop(0) if fixture.status_codes else 200
                else:
                    fixture.received.append(json.loads(body))
                    if self.path.endswith('/metrics') and fixture.metrics_failure_reported is not None:
                        # 실패 보고가 수신되기 전에 수집기가 복구 상태로 바뀌지 않게 한다.
                        code = 200 if fixture.metrics_failure_reported.is_set() else 401
                    else:
                        code = fixture.codes.pop(0) if fixture.codes else 200
                if fixture.delay:
                    time.sleep(fixture.delay)
                self.reply(code, b'{}')

            def reply(self, code, body):
                self.send_response(code)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                try:
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=lambda: self.server.serve_forever(poll_interval=0.01), daemon=True)
        self.thread.start()
        self.url = 'http://127.0.0.1:' + str(self.server.server_port)
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.addCleanup(self.close_server)

    def close_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def process(self, source, token='test-token', report_status=False):
        log = Path(self.directory.name) / 'application.log'
        log.write_text('2026-11-15T00:00:00.000Z ERROR test failure\n'
                       'java.lang.RuntimeException: expected\n\tat Example.run(Example.java:1)\n'
                       '2026-11-15T00:00:01.000Z INFO recovered\n')
        self.path = Path(self.directory.name) / 'status.json'
        env = dict(os.environ, SERVER_NAME='evaluation-test', MCP_TOKEN=token,
                   LOG_PATH=str(log) if source == 'logs' else '',
                   ACTUATOR_URL=self.url + '/metrics' if source == 'metrics' else '',
                   HEALTH_URL=self.url + '/health' if source == 'health' else '',
                   MCP_LOG_INGEST_URL=self.url + '/ingest/logs',
                   MCP_METRIC_INGEST_URL=self.url + '/ingest/metrics',
                   MCP_HEALTH_INGEST_URL=self.url + '/ingest/health',
                   MCP_FORWARDER_STATUS_INGEST_URL=self.url + '/ingest/forwarder-status' if report_status else '',
                   FORWARDER_REPORT_INTERVAL='1', FORWARDER_VERSION='integration-test',
                   DISCORD_WEBHOOK_URL='', START_AT_END='false', FLUSH_INTERVAL_MS='50',
                   METRIC_INTERVAL='1', HEALTH_INTERVAL='1', FORWARDER_STATUS_INTERVAL='1',
                   FORWARDER_STATUS_PATH=str(self.path), NO_PROXY='127.0.0.1,localhost')
        process = subprocess.Popen([sys.executable, '-u', 'main.py'], cwd=ROOT, env=env,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.addCleanup(self.stop_process, process)
        self.child_env = env
        return process

    def stop_process(self, process):
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)

    def wait_status(self, predicate):
        deadline = time.monotonic() + 8
        last = None
        while time.monotonic() < deadline:
            try:
                last = json.loads(self.path.read_text())
                if predicate(last):
                    return last
            except (OSError, ValueError):
                pass
            time.sleep(0.05)
        self.fail('Expected status not observed: ' + repr(last))

    def test_상태보고는_JSON형식을_요구하는_실제HTTP서버에_전송된다(self):
        with patch.object(status_reporter, 'INGEST_URL', self.url + '/ingest/forwarder-status'), \
             patch.object(status_reporter, 'TOKEN', 'test-token'):
            result = status_reporter.report_once(1)
        self.assertTrue(result.success)
        self.assertEqual(result.status_code, 200)
        self.assertEqual(len(self.status_received), 1)
        self.assertEqual(self.status_received[0]['sequence'], 1)

    def test_실제HTTP에서_500후_같은본문으로_복구한다(self):
        self.codes = [500, 200]
        registry = StatusRegistry()
        with patch.object(delivery, 'STATUS', registry):
            result = delivery.post_json('metrics', self.url, {'X-MCP-TOKEN': 'test'},
                                        {'eventId': 'same'}, initial_backoff=0)
        self.assertTrue(result.success)
        self.assertEqual(self.received, [{'eventId': 'same'}, {'eventId': 'same'}])
        self.assertEqual(registry.snapshot()['streams']['metrics']['sent_items'], 1)

    def test_실제HTTP_시간초과를_두번만_시도한다(self):
        self.delay = 0.1
        with patch.object(delivery, 'STATUS', StatusRegistry()):
            result = delivery.post_json('health', self.url, {'X-MCP-TOKEN': 'test'}, {},
                                        timeout=0.02, max_attempts=2, initial_backoff=0)
        self.assertFalse(result.success)
        self.assertEqual(result.reason, 'timeout')
        self.assertEqual(result.attempts, 2)

    def test_로그파일부터_HTTP수신과_상태파일까지_연결된다(self):
        self.process('logs')
        snapshot = self.wait_status(lambda value: value['streams']['logs']['sent_items'] >= 2)
        events = [event for batch in self.received for event in batch]
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]['level'], 'ERROR')
        self.assertIn('RuntimeException', events[0]['message'])
        self.assertIn('Example.java:1', events[0]['message'])
        self.assertTrue(healthy(snapshot))
        self.assertEqual(subprocess.run([sys.executable, 'healthcheck.py'], cwd=ROOT,
                                        env=self.child_env, timeout=2).returncode, 0)

    def test_실제401은_상태파일과_헬스체크에_실패로_반영된다(self):
        self.codes = [401] * 20
        self.process('metrics')
        snapshot = self.wait_status(lambda value: value['streams']['metrics']['dropped_items'] >= 1)
        self.assertEqual(snapshot['streams']['metrics']['sent_items'], 0)
        self.assertEqual(snapshot['streams']['metrics']['last_send_error'], 'authentication_failed')
        self.assertFalse(healthy(snapshot))
        self.assertEqual(subprocess.run([sys.executable, 'healthcheck.py'], cwd=ROOT,
                                        env=self.child_env, timeout=2).returncode, 1)

    def test_대상DOWN관측을_전송한_수집기는_동작정상이다(self):
        self.process('health')
        snapshot = self.wait_status(lambda value: value['streams']['health']['sent_items'] >= 1)
        self.assertEqual(self.received[0]['status'], 'DOWN')
        self.assertTrue(healthy(snapshot))

    def test_수집스레드_설정오류가_프로세스에_가려지지않는다(self):
        process = self.process('health', token='')
        snapshot = self.wait_status(lambda value: value['streams']['health']['state'] == 'failed')
        self.assertIsNone(process.poll())
        self.assertFalse(healthy(snapshot))
        self.assertEqual(self.received, [])

    def wait_reports(self, predicate):
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if predicate(self.status_received):
                return self.status_received
            time.sleep(0.02)
        self.fail('Expected report not received: ' + repr(self.status_received))

    def test_인증실패보고를_받기전까지_지표요청에_401을_유지한다(self):
        self.metrics_failure_reported = threading.Event()

        def post(path, payload):
            with requests.post(self.url + path, json=payload, timeout=2) as response:
                return response.status_code

        for _ in range(3):
            self.assertEqual(post('/ingest/metrics', {}), 401)
        post('/ingest/forwarder-status', {'streams': {'metrics': {'last_send_error': None}}})
        self.assertEqual(post('/ingest/metrics', {}), 401)
        post('/ingest/forwarder-status',
             {'streams': {'metrics': {'last_send_error': 'authentication_failed'}}})
        self.assertTrue(self.metrics_failure_reported.is_set())
        self.assertEqual(post('/ingest/metrics', {}), 200)

    def test_실제프로세스의_수집실패를_독립보고하고_복구후_최신상태로_교체한다(self):
        self.metrics_failure_reported = threading.Event()
        self.process('metrics', report_status=True)
        reports = self.wait_reports(lambda values: any(value['streams']['metrics']['last_send_error'] == 'authentication_failed' for value in values))
        failed = next(value for value in reports if value['streams']['metrics']['last_send_error'] == 'authentication_failed')
        reports = self.wait_reports(lambda values: any(value['sequence'] > failed['sequence'] and value['streams']['metrics']['last_send_error'] is None and value['streams']['metrics']['sent_items'] > 0 for value in values))
        recovered = reports[-1]
        self.assertEqual(recovered['run_id'], failed['run_id'])
        self.assertGreaterEqual(recovered['streams']['metrics']['dropped_items'], 1)
        self.assertEqual(set(recovered['streams']), {'logs', 'log_sender', 'metrics', 'health'})

    def test_상태API가_실패해도_로그전송과_로컬검사는_계속된다(self):
        self.status_codes = [500] * 20
        self.process('logs', report_status=True)
        snapshot = self.wait_status(lambda value: value['streams']['logs']['sent_items'] >= 2)
        self.wait_reports(lambda values: len(values) >= 2)
        self.assertTrue(healthy(snapshot))
        self.assertEqual(snapshot['streams']['logs']['dropped_items'], 0)
        self.assertEqual(len(self.received), 1)

    def test_프로세스재시작은_새실행ID를_사용한다(self):
        process = self.process('health', report_status=True)
        first = self.wait_reports(lambda values: len(values) >= 1)[0]
        self.stop_process(process)
        self.status_received.clear()
        self.process('health', report_status=True)
        second = self.wait_reports(lambda values: len(values) >= 1)[0]
        self.assertNotEqual(first['run_id'], second['run_id'])
        self.assertEqual(second['sequence'], 1)
        self.assertGreater(second['started_at'], first['started_at'])
