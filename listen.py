import logging

from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError

from kg_job import ZK_HOST, ZK_ROOT
from kg_job.Basic_kg import Basic_kg

from daemon import DaemonContext


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
        logging.info(job)
        zk.delete(ZK_ROOT + "start/" + job)
        with DaemonContext():
            run(job)


@zk.ChildrenWatch(ZK_ROOT + "stop")
def start_watch(children):
    print(children)


if __name__ == '__main__':
    try:
        zk.start()
    except KazooTimeoutError as e:
        e.__traceback__
        exit("can't connect zookeeper")
    input()