import logging

from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.exceptions import NoNodeError

from kg_job import ZK_HOST, ZK_ROOT
from kg_job.Basic_kg import Basic_kg

import os
import signal


def run(job):
    if job == "tushare-basic":
        Basic_kg.run_tushare_basic()
    elif job == "jrj-product":
        Basic_kg.run_jrj_product()
    elif job == "jrj-holder":
        Basic_kg.run_jrj_holder()
    elif job == "jrj-report-topic":
        Basic_kg.run_jrj_report_topic()
    elif job == "jrj-report-content":
        Basic_kg.run_jrj_report_content(10000)
    elif job == "jrj-news-topic":
        Basic_kg.run_jrj_news_topic(False)
    elif job == "jrj-news-content":
        Basic_kg.run_jrj_news_content(10000)
    else:
        pass


zk = KazooClient(hosts=ZK_HOST)


@zk.ChildrenWatch(ZK_ROOT + "start")
def start_watch(children):
    for job in children:
        logging.info("start {}".format(job))
        zk.delete(ZK_ROOT + "start/" + job)
        pid = os.fork()
        if pid == 0:
            run(job)
            exit(0)
        else:
            zk.create(ZK_ROOT + "pid/" + job, str(pid).encode())


@zk.ChildrenWatch(ZK_ROOT + "stop")
def stop_watch(children):
    for job in children:
        logging.info("stop {}".format(job))
        zk.delete(ZK_ROOT + "stop/" + job)
        try:
            pid = int(zk.get(ZK_ROOT + "pid/" + job)[0].decode())
            zk.delete(ZK_ROOT + "pid/" + job)
            os.kill(pid, signal.SIGKILL)
        except NoNodeError as no_node_err:
            no_node_err.__traceback__


if __name__ == '__main__':
    try:
        zk.start()
    except KazooTimeoutError as e:
        e.__traceback__
        exit("can't connect zookeeper")
    if not zk.exists(ZK_ROOT + "start"):
        zk.create(ZK_ROOT + "start")
    if not zk.exists(ZK_ROOT + "stop"):
        zk.create(ZK_ROOT + "stop")
    if not zk.exists(ZK_ROOT + "pid"):
        zk.create(ZK_ROOT + "pid")

    input()
    zk.close()
