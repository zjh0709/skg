from jiucai_knowledge_graph.service.watcher import JobWatcher
from daemon import DaemonContext
import time
import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.parse_args()
    parser.add_argument("-n", "--name", help="service name")
    parser.add_argument("-a", "--action", help="start|stop")

    if parser.name == "watcher" and parser.action == "start":
        with DaemonContext():
            JobWatcher.start()
            while True:
                time.sleep(1)
    elif parser.name == "watcher" and parser.action == "stop":
        JobWatcher.stop()
