from jiucai_knowledge_graph.service import settings
import logging
import time


class JobWatcher(object):
    def __init__(self):
        self.stdin_path = settings.get("stdin_path")
        self.stdout_path = settings.get("stdout_path")
        self.stderr_path = settings.get("stderr_path")
        self.pidfile_path = settings.get("pidfile_path")
        self.pidfile_timeout = settings.get("pidfile_timeout")

    def run(self):
        logging.info("now is {}".format(time.ctime()))

