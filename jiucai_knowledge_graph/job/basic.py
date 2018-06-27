import os
import signal
import logging
from jiucai_knowledge_graph import ZK_ROOT
from jiucai_knowledge_graph.util.DataUtil import DataUtil
from jiucai_knowledge_graph.util.ZkUtil import ZkUtil
from jiucai_knowledge_graph.resource import tu, sina, jrj, hexun
from concurrent.futures import ThreadPoolExecutor


class BasicJob(object):
    def __init__(self, job_name: str):
        self.data_util = DataUtil()
        self.zk_util = ZkUtil()
        self.zk_status_path = ZK_ROOT + "job/" + job_name + "/status"
        self.zk_total_path = ZK_ROOT + "job/" + job_name + "/total"
        self.zk_counter_path = ZK_ROOT + "job/" + job_name + "/counter"
        self.threading_num_low = 2
        self.threading_num_high = 6
        if self.zk_util.exists(self.zk_status_path):
            self.zk_util.stop()
            logging.info("the last job is still running.")
            logging.info("will kill this application, pid {}".format(os.getpid()))
            os.kill(os.getpid(), signal.SIGKILL)
        else:
            self.zk_util.create_ephemeral(self.zk_status_path, "running")
        self.counter = self.zk_util.counter(self.zk_counter_path)

    def tushare_basic(self) -> None:
        nodes, links, data = tu.get_stock_basic()
        self.zk_util.create(self.zk_total_path, len(nodes) + len(links) + len(data))
        with ThreadPoolExecutor(max_workers=self.threading_num_low) as executor:
            executor.map(self.data_util.save_node, nodes)
        self.counter += len(nodes)
        with ThreadPoolExecutor(max_workers=self.threading_num_low) as executor:
            executor.map(self.data_util.save_link, links)
        self.counter += len(links)
        with ThreadPoolExecutor(max_workers=self.threading_num_low) as executor:
            executor.map(self.data_util.save_info, data)
        self.counter += len(data)
        self.zk_util.stop()

    def tushare_news_topic(self, num: int):
        articles = tu.get_news_topic(num)

        if not articles:
            logging.warning("data is None.")
            self.zk_util.stop()
            return None
        self.zk_util.create(self.zk_total_path, len(articles))

        def run_one(article: dict):
            content = tu.get_news_content(article["url"])
            self.data_util.save_article(content)
            self.counter += 1

        with ThreadPoolExecutor(max_workers=self.threading_num_high) as executor:
            executor.map(run_one, articles)
        self.zk_util.stop()

    def tushare_news_content(self, num: int):
        articles = self.data_util.get_articles(where={"source": "tu", "type": "news", "content": {"$exists": False}},
                                               filed={"_id": 0, "url": 1},
                                               limit=num)
        self.zk_util.create(self.zk_total_path, len(articles))

        def run_one(article: dict):
            content = tu.get_news_content(article["url"])
            self.data_util.save_article(content)
            self.counter += 1

        with ThreadPoolExecutor(max_workers=self.threading_num_high) as executor:
            executor.map(run_one, articles)
        self.zk_util.stop()

    def sina_concept(self):
        stocks = self.data_util.get_stocks()
        self.zk_util.create(self.zk_total_path, len(stocks))

        def run_one(code: str):
            nodes, links = sina.get_concept(code)
            for node in nodes:
                self.data_util.save_node(node)
            for link in links:
                self.data_util.save_link(link)
            self.counter += 1

        with ThreadPoolExecutor(max_workers=self.threading_num_low) as executor:
            executor.map(run_one, stocks)
        self.zk_util.stop()

    def jrj_product(self):
        stocks = self.data_util.get_stocks()
        self.zk_util.create(self.zk_total_path, len(stocks))

        def run_one(code: str):
            nodes, links = jrj.get_product(code)
            for node in nodes:
                self.data_util.save_node(node)
            for link in links:
                self.data_util.save_link(link)
            self.counter += 1

        with ThreadPoolExecutor(max_workers=self.threading_num_high) as executor:
            executor.map(run_one, stocks)
        self.zk_util.stop()

    def jrj_holder(self):
        stocks = self.data_util.get_stocks()
        self.zk_util.create(self.zk_total_path, len(stocks))

        def run_one(code: str):
            nodes, links = jrj.get_holder(code)
            for node in nodes:
                self.data_util.save_node(node)
            for link in links:
                self.data_util.save_link(link)
            self.counter += 1

        with ThreadPoolExecutor(max_workers=self.threading_num_high) as executor:
            executor.map(run_one, stocks)
        self.zk_util.stop()

    def jrj_report_topic(self):
        stocks = self.data_util.get_stocks()
        self.zk_util.create(self.zk_total_path, len(stocks))

        def run_one(code: str):
            nodes, links, articles = jrj.get_report_topic(code)
            for node in nodes:
                self.data_util.save_node(node)
            for link in links:
                self.data_util.save_link(link)
            for article in articles:
                self.data_util.save_article(article)
            self.counter += 1

        with ThreadPoolExecutor(max_workers=self.threading_num_high) as executor:
            executor.map(run_one, stocks)
        self.zk_util.stop()

    def jrj_report_content(self, num: int):
        articles = self.data_util.get_articles(where={"source": "jrj", "type": "report", "content": {"$exists": False}},
                                               filed={"_id": 0, "url": 1},
                                               limit=num)
        self.zk_util.create(self.zk_total_path, len(articles))

        def run_one(article: dict):
            content = jrj.get_report_content(article["url"])
            self.data_util.save_article(content)
            self.counter += 1

        with ThreadPoolExecutor(max_workers=self.threading_num_high) as executor:
            executor.map(run_one, articles)
        self.zk_util.stop()

    def jrj_news_topic(self, recover=False):
        stocks = self.data_util.get_stocks()
        self.zk_util.create(self.zk_total_path, len(stocks))

        def run_one(code: str):
            articles, max_page = jrj.get_news_topic(code, 1)
            for article in articles:
                self.data_util.save_article(article)
            if recover is True and max_page > 1:
                for i in range(2, max_page + 1):
                    articles, _ = jrj.get_news_topic(code, i)
                    for article in articles:
                        self.data_util.save_article(article)
            self.counter += 1

        with ThreadPoolExecutor(max_workers=self.threading_num_high) as executor:
            executor.map(run_one, stocks)
        self.zk_util.stop()

    def jrj_news_content(self, num: int):
        articles = self.data_util.get_articles(where={"source": "jrj", "type": "news", "content": {"$exists": False}},
                                               filed={"_id": 0, "url": 1},
                                               limit=num)
        self.zk_util.create(self.zk_total_path, len(articles))

        def run_one(article: dict):
            content = jrj.get_news_content(article["url"])
            self.data_util.save_article(content)
            self.counter += 1

        with ThreadPoolExecutor(max_workers=self.threading_num_high) as executor:
            executor.map(run_one, articles)
        self.zk_util.stop()

    def hexun_chain(self):
        nodes, links, data = hexun.get_chain_topic()
        self.zk_util.create(self.zk_total_path, 2+len(data))
        for node in nodes:
            self.data_util.save_node(node)
        self.counter += 1
        for link in links:
            self.data_util.save_link(link)
        self.counter += 1

        def run_one(d: dict):
            nodes_, links_ = hexun.get_chain_content(d)
            for node_ in nodes_:
                self.data_util.save_node(node_)
            for link_ in links_:
                self.data_util.save_link(link_)
            self.counter += 1

        with ThreadPoolExecutor(max_workers=self.threading_num_high) as executor:
            executor.map(run_one, data)
        self.zk_util.stop()

    @staticmethod
    def run(job_name, **kwargs):
        getattr(BasicJob(job_name), job_name)(**kwargs)


if __name__ == '__main__':
    BasicJob.run("tushare_news_topic", num=10)
