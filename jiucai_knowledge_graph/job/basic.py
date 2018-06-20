from jiucai_knowledge_graph import MONGODB_HOST, MONGODB_PORT, MONGODB_DB, ZK_HOST, ZK_ROOT
from jiucai_knowledge_graph.resource import tu, sina, jrj
import pymongo
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError
from concurrent.futures import ThreadPoolExecutor
import logging
from functools import partial
import time


class BasicJob(object):
    def __init__(self, zk_node: str):
        self.client = pymongo.MongoClient(host=MONGODB_HOST, port=MONGODB_PORT, connect=False)
        self.db = self.client.get_database(MONGODB_DB)
        self.zk_job_node = ZK_ROOT + "job"
        self.zk_status_node = self.zk_job_node + "/" + zk_node + "/status"
        self.zk_total_node = self.zk_job_node + "/" + zk_node + "/total"
        self.zk_counter_node = self.zk_job_node + "/" + zk_node + "/counter"
        self.zk = KazooClient(hosts=ZK_HOST)
        self.zk_start()
        self.counter = self.zk_get_counter()
        self.threading_num_low = 2
        self.threading_num_high = 6

    def zk_start(self):
        try:
            self.zk.start()
            if self.zk.exists(self.zk_status_node):
                self.zk.stop()
                exit("job is still running!")
            else:
                if not self.zk.exists(self.zk_job_node):
                    self.zk.create(self.zk_job_node, b'job')
                self.zk.create(path=self.zk_status_node,
                               value=b"running",
                               ephemeral=True,
                               makepath=True)
        except KazooTimeoutError as e:
            self.zk = None
            e.__traceback__

    def zk_stop(self):
        if self.zk is not None:
            for node in [self.zk_status_node, self.zk_total_node, self.zk_counter_node]:
                if self.zk.exists(node):
                    self.zk.delete(node)
            self.zk.stop()

    def zk_get_counter(self):
        counter = 0
        if self.zk is not None:
            if self.zk.exists(self.zk_counter_node):
                self.zk.delete(self.zk_counter_node)
            counter = self.zk.Counter(self.zk_counter_node)
        else:
            pass
        return counter

    def zk_set_total(self, num: int):
        if self.zk is not None:
            if self.zk.exists(self.zk_total_node):
                self.zk.delete(self.zk_total_node)
            self.zk.create(path=self.zk_total_node,
                           value=str(num).encode(),
                           makepath=True)

    def update(self, collection_name: str, spec: dict, document: dict, upsert=True):
        document.setdefault("timestamp", int(time.time() * 1000))
        self.db.get_collection(collection_name).update(spec, {"$set": document}, upsert)
        logging.info(spec)

    def tushare_basic(self) -> None:
        entity, relation, data = tu.get_stock_basic()
        self.zk_set_total(len(entity)+len(relation)+len(data))
        entity_update = partial(self.update, "entity")
        with ThreadPoolExecutor(max_workers=self.threading_num_low) as executor:
            executor.map(entity_update,
                         map(lambda x: {"name": x.name}, entity),
                         map(lambda x: {"name": x.name, "type": x.type}, entity))
        self.counter += len(entity)
        relation_update = partial(self.update, "relation")
        with ThreadPoolExecutor(max_workers=self.threading_num_low) as executor:
            executor.map(relation_update,
                         map(lambda x: {"head": x.head, "tail": x.tail}, relation),
                         map(lambda x: {"head": x.head, "relation": x.relation, "tail": x.tail}, relation))
        self.counter += len(relation)
        basic_update = partial(self.update, "basic")
        with ThreadPoolExecutor(max_workers=self.threading_num_low) as executor:
            executor.map(basic_update,
                         map(lambda x: {"code": x["code"]}, data),
                         data)
        self.counter += len(data)
        self.zk_stop()

    def sina_concept(self):
        stock = self.db.get_collection("basic").find({}, {"_id": 0, "code": 1})
        stock = list(stock)
        self.zk_set_total(len(stock))

        def run_one(code: str):
            entity, relation = sina.get_concept(code)
            for x in entity:
                self.update("entity",
                            {"name": x.name},
                            {"name": x.name, "type": x.type},
                            upsert=True)
            for x in relation:
                self.update("relation",
                            {"head": x.head, "tail": x.tail},
                            {"head": x.head, "relation": x.relation, "tail": x.tail},
                            upsert=True)
            self.counter += 1

        with ThreadPoolExecutor(max_workers=self.threading_num_low) as executor:
            executor.map(run_one,
                         list(map(lambda x: x["code"], stock)))
        self.zk_stop()

    def sina_holder(self):
        stock = self.db.get_collection("basic").find({}, {"_id": 0, "code": 1})
        stock = list(stock)
        self.zk_set_total(len(stock))

        def run_one(code: str):
            entity, relation = sina.get_holder(code)
            for x in entity:
                self.update("entity",
                            {"name": x.name},
                            {"name": x.name, "type": x.type},
                            upsert=True)
            for x in relation:
                self.update("relation",
                            {"head": x.head, "tail": x.tail},
                            {"head": x.head, "relation": x.relation, "tail": x.tail, "extend": x.extend},
                            upsert=True)
            self.counter += 1

        with ThreadPoolExecutor(max_workers=self.threading_num_low) as executor:
            executor.map(run_one,
                         list(map(lambda x: x["code"], stock)))
        self.zk_stop()

    def jrj_product(self):
        stock = self.db.get_collection("basic").find({}, {"_id": 0, "code": 1})
        stock = list(stock)
        self.zk_set_total(len(stock))

        def run_one(code: str):
            entity, relation = jrj.get_product(code)
            for x in entity:
                self.update("entity",
                            {"name": x.name},
                            {"name": x.name, "type": x.type},
                            upsert=True)
            for x in relation:
                self.update("relation",
                            {"head": x.head, "tail": x.tail},
                            {"head": x.head, "relation": x.relation, "tail": x.tail, "extend": x.extend},
                            upsert=True)
            self.counter += 1

        with ThreadPoolExecutor(max_workers=self.threading_num_high) as executor:
            executor.map(run_one,
                         list(map(lambda x: x["code"], stock)))
        self.zk_stop()

    def jrj_holder(self):
        stock = self.db.get_collection("basic").find({}, {"_id": 0, "code": 1})
        stock = list(stock)
        self.zk_set_total(len(stock))

        def run_one(code: str):
            entity, relation = jrj.get_holder(code)
            for x in entity:
                self.update("entity",
                            {"name": x.name},
                            {"name": x.name, "type": x.type},
                            upsert=True)
            for x in relation:
                self.update("relation",
                            {"head": x.head, "tail": x.tail},
                            {"head": x.head, "relation": x.relation, "tail": x.tail, "extend": x.extend},
                            upsert=True)
            self.counter += 1

        with ThreadPoolExecutor(max_workers=self.threading_num_high) as executor:
            executor.map(run_one,
                         list(map(lambda x: x["code"], stock)))
        self.zk_stop()

    def jrj_report_topic(self):
        stock = self.db.get_collection("basic").find({}, {"_id": 0, "code": 1})
        stock = list(stock)
        self.zk_set_total(len(stock))

        def run_one(code: str):
            entity, relation, topic = jrj.get_report_topic(code)
            for x in entity:
                self.update("entity",
                            {"name": x.name},
                            {"name": x.name, "type": x.type},
                            upsert=True)
            for x in relation:
                self.update("relation",
                            {"head": x.head, "tail": x.tail},
                            {"head": x.head, "relation": x.relation, "tail": x.tail},
                            upsert=True)
            for x in topic:
                self.update("article",
                            {"url": x["url"]},
                            x,
                            upsert=True)
            self.counter += 1

        with ThreadPoolExecutor(max_workers=self.threading_num_high) as executor:
            executor.map(run_one,
                         list(map(lambda x: x["code"], stock)))
        self.zk_stop()

    def jrj_report_content(self, num: int):
        url = self.db.get_collection("article").find({"source": "jrj", "type": "report", "content": {"$exists": False}},
                                                     {"_id": 0, "url": 1}).limit(num)
        url = list(url)
        self.zk_set_total(len(url))

        def run_one(url_: str):
            data = jrj.get_report_content(url_)
            self.update("article",
                        {"url": url_},
                        data,
                        upsert=False)
            self.counter += 1

        with ThreadPoolExecutor(max_workers=self.threading_num_high) as executor:
            executor.map(run_one,
                         list(map(lambda x: x["url"], url)))
        self.zk_stop()

    def jrj_news_topic(self, recover=False):
        stock = self.db.get_collection("basic").find({}, {"_id": 0, "code": 1})
        stock = list(stock)
        self.zk_set_total(len(stock))

        def run_one(code: str):
            links, max_page = jrj.get_news_topic(code, 1)
            for x in links:
                self.update("article",
                            {"url": x["url"]},
                            x,
                            upsert=True)
            if recover is True and max_page > 1:
                for i in range(2, max_page + 1):
                    links, _ = jrj.get_news_topic(code, 1)
                    for x in links:
                        self.update("article",
                                    {"url": x["url"]},
                                    x,
                                    upsert=True)
            self.counter += 1

        with ThreadPoolExecutor(max_workers=self.threading_num_high) as executor:
            executor.map(run_one,
                         list(map(lambda x: x["code"], stock)))
        self.zk_stop()

    def jrj_news_content(self, num: int):
        url = self.db.get_collection("article").find({"source": "jrj", "type": "news", "content": {"$exists": False}},
                                                     {"_id": 0, "url": 1}).limit(num)
        url = list(url)
        self.zk_set_total(len(url))

        def run_one(url_: str):
            data = jrj.get_news_content(url_)
            self.update("article",
                        {"url": url_},
                        data,
                        upsert=False)
            self.counter += 1

        with ThreadPoolExecutor(max_workers=self.threading_num_high) as executor:
            executor.map(run_one,
                         list(map(lambda x: x["url"], url)))
        self.zk_stop()

    @staticmethod
    def run_tushare_basic():
        kg = BasicJob("tushare_basic")
        kg.tushare_basic()

    @staticmethod
    def run_sina_concept():
        kg = BasicJob("sina_concept")
        kg.sina_concept()

    @staticmethod
    def run_sina_holder():
        kg = BasicJob("sina_holder")
        kg.sina_holder()

    @staticmethod
    def run_jrj_product():
        kg = BasicJob("jrj_product")
        kg.jrj_product()

    @staticmethod
    def run_jrj_holder():
        kg = BasicJob("jrj_holder")
        kg.jrj_holder()

    @staticmethod
    def run_jrj_report_topic():
        kg = BasicJob("jrj_report_topic")
        kg.jrj_report_topic()

    @staticmethod
    def run_jrj_report_content(num: int):
        kg = BasicJob("jrj_report_content")
        kg.jrj_report_content(num)

    @staticmethod
    def run_jrj_news_topic(recover=False):
        kg = BasicJob("jrj_news_topic")
        kg.jrj_news_topic(recover)

    @staticmethod
    def run_jrj_news_content(num: int):
        kg = BasicJob("jrj_news_content")
        kg.jrj_news_content(num)


if __name__ == '__main__':
    BasicJob.run_jrj_news_topic()
