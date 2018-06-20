from jiucai_knowledge_graph.service import settings
from daemon import DaemonContext
import logging
import time


class JobWatcher(object):
    def run(self):
        with DaemonContext():
            f = open(settings.get("stdout_path"), "w")
            f.write("{} \n".format(time.ctime()))
            time.sleep(1)

