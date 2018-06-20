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
        self.zk_service_node = ZK_ROOT + "service"
        self.zk_start_node = ZK_ROOT + "start"
        self.zk_stop_node = ZK_ROOT + "stop"
        self.zk_pid_node = ZK_ROOT + "pid"
        self.zk_node = self.zk_service_node + "/job-watcher"
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

    def register_service(self):
        if not self.zk.exists(self.zk_service_node):
            self.zk.create(self.zk_service_node, b"service")
        if self.zk.exists(self.zk_node):
            return False
        else:
            self.zk.create(self.zk_node, str(os.getpid()).encode())
            return True

    def start(self):
        if not self.register_service():
            exit("service is already running.")

        @self.zk.ChildrenWatch(self.zk_start_node)
        def start_watch(children):
            for job in children:
                logging.info("start {}".format(job))
                self.zk.delete(self.zk_start_node + "/" + job)
                pid = os.fork()
                if pid == 0:
                    self.runner(job)
                    exit(0)
                else:
                    self.zk.create(self.zk_pid_node + "/" + job, str(pid).encode())

        @self.zk.ChildrenWatch(self.zk_stop_node)
        def stop_watch(children):
            for job in children:
                logging.info("stop {}".format(job))
                self.zk.delete(self.zk_stop_node + "/" + job)
                try:
                    pid = int(self.zk.get(self.zk_pid_node + "/" + job)[0].decode())
                    self.zk.delete(self.zk_pid_node + "/" + job)
                    os.kill(pid, signal.SIGKILL)
                except NoNodeError as no_node_err:
                    no_node_err.__traceback__

    def stop(self):
        pid = int(self.zk.get(self.zk_node)[0].decode())
        self.zk.delete(self.zk_node)
        os.kill(pid, signal.SIGKILL)
