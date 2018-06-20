from jiucai_knowledge_graph.service.watcher import JobWatcher
from daemon.runner import DaemonRunner


if __name__ == '__main__':
    daemon_runner = DaemonRunner(JobWatcher())
    daemon_runner.do_action()

