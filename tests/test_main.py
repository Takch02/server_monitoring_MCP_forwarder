import contextlib
import io
import threading
import unittest
from unittest.mock import patch

import main
from forwarder_status import StatusRegistry


class MainTest(unittest.TestCase):
    def test_상태파일_IO실패후에도_다음주기에_기록을_계속한다(self):
        output = io.StringIO()
        with patch.dict('os.environ', {'ACTUATOR_URL': 'http://example.invalid/metrics'}, clear=True), \
             patch.object(main, 'STATUS', StatusRegistry()), \
             patch.object(main.status_reporter, 'INGEST_URL', None), \
             patch.object(main.forwarder, 'stop_event', threading.Event()), \
             patch.object(main.threading, 'Thread') as thread, \
             patch.object(main, 'write_status', side_effect=[OSError('sensitive-path'), None]) as write, \
             patch.object(main.time, 'sleep', side_effect=[None, KeyboardInterrupt]), \
             contextlib.redirect_stdout(output):
            main.main()
        self.assertEqual(write.call_count, 2)
        self.assertEqual(thread.return_value.start.call_count, 1)
        self.assertNotIn('sensitive-path', output.getvalue())
