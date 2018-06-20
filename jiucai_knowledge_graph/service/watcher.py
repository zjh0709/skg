from jiucai_knowledge_graph.service import settings
from daemon import DaemonContext
import logging
import time


class JobWatcher(object):
    def run(self):
        with DaemonContext():
            while True:
                logging.info(time.ctime())
                time.sleep(1)

