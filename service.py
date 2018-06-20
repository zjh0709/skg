from jiucai_knowledge_graph.service.watcher import JobWatcher
from daemon import DaemonContext
import time
from argparse import ArgumentParser
import sys


if __name__ == '__main__':
    parser = ArgumentParser(usage="%s service.py" % sys.executable,
                            description="run service background.",
                            epilog="use start|stop action.")
    parser.add_argument("--name", dest="name", help="service name")
    parser.add_argument("--action", dest="action", help="start|stop")
    args = parser.parse_args()

    if parser.name == "watcher" and parser.action == "start":
        with DaemonContext():
            JobWatcher.start()
            while True:
                time.sleep(1)
    elif parser.name == "watcher" and parser.action == "stop":
        JobWatcher.stop()
