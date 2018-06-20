from jiucai_knowledge_graph.service.watcher import JobWatcher
from daemon import DaemonContext
import time


if __name__ == '__main__':
    with DaemonContext():
        job_watcher = JobWatcher()
        job_watcher.run()
        while True:
            time.sleep(1)

