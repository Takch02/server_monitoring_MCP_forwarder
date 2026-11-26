import unittest

from log_forwarder import make_event_id


class LogIdentityTest(unittest.TestCase):
    def test_같은파일_같은시각의_동일한_두줄과_재처리(self):
        args = ('demo-server', 1000, '캐시 미스', 77)
        first = make_event_id(*args, 0)
        second = make_event_id(*args, 40)
        self.assertNotEqual(first, second)
        self.assertEqual(first, make_event_id(*args, 0))
