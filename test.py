from jiucai_knowledge_graph.service.watcher import JobWatcher
from daemon import DaemonContext


if __name__ == '__main__':
    with DaemonContext():
        job_watcher = JobWatcher()
        job_watcher.run()

