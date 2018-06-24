from jiucai_knowledge_graph.service.watcher import JobWatcher
from daemon import DaemonContext
import time
from argparse import ArgumentParser
import sys
import logging


if __name__ == '__main__':
    parser = ArgumentParser(usage="%s service.py" % sys.executable,
                            description="run service background.",
                            epilog="use start|stop action.")
    parser.add_argument("--name", dest="name", help="service name")
    parser.add_argument("--action", dest="action", help="start|stop")
    args = parser.parse_args()

    if args.name == "watcher" and args.action == "start":
        logging.info("{} {}".format(args.name, args.action))
        with DaemonContext():
            JobWatcher.start()
            while True:
                time.sleep(100)
    elif args.name == "watcher" and args.action == "stop":
        logging.info("{} {}".format(args.name, args.action))
        JobWatcher.stop()
