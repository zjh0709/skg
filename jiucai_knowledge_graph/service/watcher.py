from jiucai_knowledge_graph import ZK_ROOT, ZK_HOST
from jiucai_knowledge_graph.job.basic import BasicJob
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.exceptions import NoNodeError
import logging
import os
import signal

from jiucai_knowledge_graph.util.ZkUtil import ZkUtil


class JobWatcher(object):
    def __init__(self):
        self.zk_util = ZkUtil()
        self.zk_service_path = ZK_ROOT + "service/job_watcher"
        self.zk_start_path = ZK_ROOT + "start"
        self.zk_stop_path = ZK_ROOT + "stop"
        self.zk_pid_path = ZK_ROOT + "pid"
        if self.zk_util.exists(self.zk_service_path):
            self.zk_util.stop()
            exit("service is still running")
        else:
            self.zk_util.create_ephemeral(self.zk_service_path, os.getpid())

    def runner(self, job: str):
        if job == "tushare_basic":
            BasicJob.run_tushare_basic()
        elif job == "jrj_product":
            BasicJob.run_jrj_product()
        elif job == "jrj_holder":
            BasicJob.run_jrj_holder()
        elif job == "jrj_report_topic":
            BasicJob.run_jrj_report_topic()
        elif job == "jrj_report_content":
            BasicJob.run_jrj_report_content(10000)
        elif job == "jrj_news_topic":
            BasicJob.run_jrj_news_topic(False)
        elif job == "jrj_news_content":
            BasicJob.run_jrj_news_content(10000)
        else:
            pass

    def run_start(self):
        @self.zk_util.child_watch(self.zk_start_path)
        def start_watch(children):
            for job in children:
                logging.info("start {}".format(job))
                self.zk_util.delete(self.zk_start_path + "/" + job)
                pid = os.fork()
                if pid == 0:
                    # self.runner(job)
                    print(job)
                    exit(0)
                else:
                    self.zk_util.create_ephemeral(self.zk_pid_path + "/" + job, pid)

        @self.zk_util.child_watch(self.zk_stop_path)
        def stop_watch(children):
            for job in children:
                logging.info("stop {}".format(job))
                self.zk_util.delete(self.zk_stop_path + "/" + job)
                try:
                    pid = int(self.zk_util.get_value(self.zk_pid_path + "/" + job))
                    os.kill(pid, signal.SIGKILL)
                except NoNodeError as e:
                    e.__traceback__

    def run_stop(self):
        try:
            pid = int(self.zk_util.get_value(self.zk_service_path))
            os.kill(pid, signal.SIGKILL)
        except Exception as e:
            e.__traceback__

    @staticmethod
    def start():
        job_watcher = JobWatcher()
        job_watcher.run_start()

    @staticmethod
    def stop():
        job_watcher = JobWatcher()
        job_watcher.run_stop()
