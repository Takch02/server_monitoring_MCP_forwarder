"""기존 수동 테스트 진입점에서도 현재 회귀·연결 테스트를 실행한다."""
from pathlib import Path
import sys
import unittest


def run_test():
    tests = unittest.defaultTestLoader.discover(str(Path(__file__).parent / 'tests'))
    return unittest.TextTestRunner(verbosity=2).run(tests).wasSuccessful()


if __name__ == '__main__':
    sys.exit(0 if run_test() else 1)
