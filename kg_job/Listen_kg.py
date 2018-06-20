from kg_job import ZK_HOST, ZK_ROOT
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError
from daemon.runner import DaemonRunner
import logging
import time


class Listen_kg(object):
    def __init__(self):
        self.stdin_path = "/dev/null"
        self.stdout_path = "/dev/tty"
        self.stderr_path = "/dev/tty"
        self.pidfile_path = '/tmp/foo.pid'
        self.pidfile_timeout = 5

    def run(self):
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            datefmt='%a, %d %b %Y %H:%M:%S',
                            filemode="w",
                            filename="mytest.log")
        while True:
            for i in range(20):
                logging.info("begin scan {} \n".format(i))
            time.sleep(1)


if __name__ == '__main__':
    daemon_runner = DaemonRunner(Listen_kg())
    daemon_runner.do_action()
