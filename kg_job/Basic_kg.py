from kg_job import MONGODB_HOST, MONGODB_PORT, MONGODB_DB, ZK_HOST, ZK_ROOT
from kg_resource import tu, sina, jrj
import os
import pymongo
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError
from concurrent.futures import ThreadPoolExecutor
import logging
from functools import partial
import time


class Basic_kg(object):
    def __init__(self, zk_node: str):
        self.client = pymongo.MongoClient(host=MONGODB_HOST, port=MONGODB_PORT, connect=False)
        self.db = self.client.get_database(MONGODB_DB)
        self.zk_status_node = os.path.join(ZK_ROOT, zk_node, "status").replace("\\", "/")
        self.zk_total_node = os.path.join(ZK_ROOT, zk_node, "total").replace("\\", "/")
        self.zk_counter_node = os.path.join(ZK_ROOT, zk_node, "counter").replace("\\", "/")
        self.zk = KazooClient(hosts=ZK_HOST)
        self.zk_start()
        self.counter = self.zk_counter()

    def zk_start(self):
        try:
            self.zk.start()
            if self.zk.exists(self.zk_status_node):
                self.zk.stop()
                exit("job is still running!")
            else:
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

    def zk_counter(self):
        counter = 0
        if self.zk is not None:
            if self.zk.exists(self.zk_counter_node):
                self.zk.delete(self.zk_counter_node)
            counter = self.zk.Counter(self.zk_counter_node)
        else:
            pass
        return counter

    def zk_total(self, num: int):
        if self.zk is not None:
            if self.zk.exists(self.zk_total_node):
                self.zk.delete(self.zk_total_node)
            self.zk.create(path=self.zk_total_node,
                           value=str(num).encode(),
                           makepath=True)

    def update(self, collection_name: str, spec: dict, document: dict, upsert=True, counter=False):
        document.setdefault("timestamp", int(time.time() * 1000))
        self.db.get_collection(collection_name).update(spec, {"$set": document}, upsert)
        logging.info(spec)
        if counter:
            self.counter += 1

    def tushare_basic(self) -> None:
        entity, relation, data = tu.get_stock_basic()
        entity_update = partial(self.update, "entity")
        with ThreadPoolExecutor(max_workers=3) as executor:
            executor.map(entity_update,
                         map(lambda x: {"name": x.name}, entity),
                         map(lambda x: {"name": x.name, "type": x.type}, entity))
        relation_update = partial(self.update, "relation")
        with ThreadPoolExecutor(max_workers=3) as executor:
            executor.map(relation_update,
                         map(lambda x: {"head": x.head, "tail": x.tail}, relation),
                         map(lambda x: {"head": x.head, "relation": x.relation, "tail": x.tail}, relation))
        basic_update = partial(self.update, "basic")
        with ThreadPoolExecutor(max_workers=3) as executor:
            executor.map(basic_update,
                         map(lambda x: {"code": x["code"]}, data),
                         data)
        self.zk_stop()

    def sina_concept(self):
        stock = self.db.get_collection("basic").find({}, {"_id": 0, "code": 1})
        stock = list(stock)
        self.zk_total(len(stock))

        def run_one(code: str):
            entity, relation = sina.get_concept(code)
            for x in entity:
                self.update("entity",
                            {"name": x.name},
                            {"name": x.name, "type": x.type},
                            upsert=True,
                            counter=True)
            for x in relation:
                self.update("relation",
                            {"head": x.head, "tail": x.tail},
                            {"head": x.head, "relation": x.relation, "tail": x.tail},
                            upsert=True,
                            counter=True)

        with ThreadPoolExecutor(max_workers=3) as executor:
            executor.map(run_one,
                         list(map(lambda x: x["code"], stock)))
        self.zk_stop()

    def sina_holder(self):
        stock = self.db.get_collection("basic").find({}, {"_id": 0, "code": 1})
        stock = list(stock)
        self.zk_total(len(stock))

        def run_one(code: str):
            entity, relation = sina.get_holder(code)
            for x in entity:
                self.update("entity",
                            {"name": x.name},
                            {"name": x.name, "type": x.type},
                            upsert=True,
                            counter=True)
            for x in relation:
                self.update("relation",
                            {"head": x.head, "tail": x.tail},
                            {"head": x.head, "relation": x.relation, "tail": x.tail, "extend": x.extend},
                            upsert=True,
                            counter=True)

        with ThreadPoolExecutor(max_workers=3) as executor:
            executor.map(run_one,
                         list(map(lambda x: x["code"], stock)))
        self.zk_stop()

    def jrj_product(self):
        stock = self.db.get_collection("basic").find({}, {"_id": 0, "code": 1})
        stock = list(stock)
        self.zk_total(len(stock))

        def run_one(code: str):
            entity, relation = jrj.get_product(code)
            for x in entity:
                self.update("entity",
                            {"name": x.name},
                            {"name": x.name, "type": x.type},
                            upsert=True,
                            counter=True)
            for x in relation:
                self.update("relation",
                            {"head": x.head, "tail": x.tail},
                            {"head": x.head, "relation": x.relation, "tail": x.tail, "extend": x.extend},
                            upsert=True,
                            counter=True)

        with ThreadPoolExecutor(max_workers=16) as executor:
            executor.map(run_one,
                         list(map(lambda x: x["code"], stock)))
        self.zk_stop()

    def jrj_holder(self):
        stock = self.db.get_collection("basic").find({}, {"_id": 0, "code": 1})
        stock = list(stock)
        self.zk_total(len(stock))

        def run_one(code: str):
            entity, relation = jrj.get_holder(code)
            for x in entity:
                self.update("entity",
                            {"name": x.name},
                            {"name": x.name, "type": x.type},
                            upsert=True,
                            counter=True)
            for x in relation:
                self.update("relation",
                            {"head": x.head, "tail": x.tail},
                            {"head": x.head, "relation": x.relation, "tail": x.tail, "extend": x.extend},
                            upsert=True,
                            counter=True)

        with ThreadPoolExecutor(max_workers=16) as executor:
            executor.map(run_one,
                         list(map(lambda x: x["code"], stock)))
        self.zk_stop()

    def jrj_report_topic(self):
        stock = self.db.get_collection("basic").find({}, {"_id": 0, "code": 1})
        stock = list(stock)
        self.zk_total(len(stock))

        def run_one(code: str):
            entity, relation, topic = jrj.get_report_topic(code)
            for x in entity:
                self.update("entity",
                            {"name": x.name},
                            {"name": x.name, "type": x.type},
                            upsert=True,
                            counter=True)
            for x in relation:
                self.update("relation",
                            {"head": x.head, "tail": x.tail},
                            {"head": x.head, "relation": x.relation, "tail": x.tail},
                            upsert=True,
                            counter=True)
            for x in topic:
                self.update("article",
                            {"url": x["url"]},
                            x,
                            upsert=True,
                            counter=True)

        with ThreadPoolExecutor(max_workers=16) as executor:
            executor.map(run_one,
                         list(map(lambda x: x["code"], stock)))
        self.zk_stop()

    def jrj_report_content(self, num: int):
        url = self.db.get_collection("article").find({"source": "jrj", "type": "report", "content": {"$exists": False}},
                                                     {"_id": 0, "url": 1}).limit(num)
        url = list(url)
        self.zk_total(len(url))

        def run_one(url_: str):
            data = jrj.get_report_content(url_)
            self.update("article",
                        {"url": url_},
                        data,
                        upsert=False,
                        counter=True)

        with ThreadPoolExecutor(max_workers=16) as executor:
            executor.map(run_one,
                         list(map(lambda x: x["url"], url)))
        self.zk_stop()

    @staticmethod
    def run_tushare_basic():
        kg = Basic_kg("tushare/basic")
        kg.tushare_basic()

    @staticmethod
    def run_sina_concept():
        kg = Basic_kg("sina/concept")
        kg.sina_concept()

    @staticmethod
    def run_sina_holder():
        kg = Basic_kg("sina/holder")
        kg.sina_holder()

    @staticmethod
    def run_jrj_product():
        kg = Basic_kg("jrj/product")
        kg.jrj_product()

    @staticmethod
    def run_jrj_holder():
        kg = Basic_kg("jrj/holder")
        kg.jrj_holder()

    @staticmethod
    def run_jrj_report_topic():
        kg = Basic_kg("jrj/report/topic")
        kg.jrj_report_topic()

    @staticmethod
    def run_jrj_report_content(num: int):
        kg = Basic_kg("jrj/report/content")
        kg.jrj_report_content(num)


if __name__ == '__main__':
    Basic_kg.run_tushare_basic()
