from jiucai_knowledge_graph import ZK_ROOT, ZK_HOST
from jiucai_knowledge_graph.job.basic import BasicJob
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.exceptions import NoNodeError
import logging
import os
import signal


class JobWatcher(object):
    def __init__(self):
        self.zk = KazooClient(hosts=ZK_HOST)
        try:
            self.zk.start()
        except KazooTimeoutError as e:
            e.__traceback__
            exit("can't connect zookeeper")

    def runner(self, job: str):
        if job == "tushare-basic":
            BasicJob.run_tushare_basic()
        elif job == "jrj-product":
            BasicJob.run_jrj_product()
        elif job == "jrj-holder":
            BasicJob.run_jrj_holder()
        elif job == "jrj-report-topic":
            BasicJob.run_jrj_report_topic()
        elif job == "jrj-report-content":
            BasicJob.run_jrj_report_content(10000)
        elif job == "jrj-news-topic":
            BasicJob.run_jrj_news_topic(False)
        elif job == "jrj-news-content":
            BasicJob.run_jrj_news_content(10000)
        else:
            pass

    def run(self):
        @self.zk.ChildrenWatch(ZK_ROOT + "start")
        def start_watch(children):
            for job in children:
                logging.info("start {}".format(job))
                self.zk.delete(ZK_ROOT + "start/" + job)
                pid = os.fork()
                if pid == 0:
                    self.runner(job)
                    exit(0)
                else:
                    self.zk.create(ZK_ROOT + "pid/" + job, str(pid).encode())

        @self.zk.ChildrenWatch(ZK_ROOT + "stop")
        def stop_watch(children):
            for job in children:
                logging.info("stop {}".format(job))
                self.zk.delete(ZK_ROOT + "stop/" + job)
                try:
                    pid = int(self.zk.get(ZK_ROOT + "pid/" + job)[0].decode())
                    self.zk.delete(ZK_ROOT + "pid/" + job)
                    os.kill(pid, signal.SIGKILL)
                except NoNodeError as no_node_err:
                    no_node_err.__traceback__


