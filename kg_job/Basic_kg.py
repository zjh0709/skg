from kg_job import MONGODB_HOST, MONGODB_PORT, MONGODB_DB
from kg_resource import tu, sina, jrj
import pymongo
from concurrent.futures import ThreadPoolExecutor
import logging
from functools import partial
import random
import time


class Basic_kg(object):
    def __init__(self):
        self.client = pymongo.MongoClient(host=MONGODB_HOST, port=MONGODB_PORT, connect=False)
        self.db = self.client.get_database(MONGODB_DB)

    def update(self, collection_name: str, spec: dict, document: dict, upsert=True):
        document.setdefault("timestamp", int(time.time()*1000))
        self.db.get_collection(collection_name).update(spec, {"$set": document}, upsert)
        logging.info(spec)

    def tushare_basic(self) -> None:
        entity, relation, data = tu.get_stock_basic()
        entity_update = partial(self.update, "entity")
        with ThreadPoolExecutor(max_workers=8) as executor:
            executor.map(entity_update,
                         map(lambda x: {"name": x.name}, entity),
                         map(lambda x: {"name": x.name, "type": x.type}, entity))
        relation_update = partial(self.update, "relation")
        with ThreadPoolExecutor(max_workers=8) as executor:
            executor.map(relation_update,
                         map(lambda x: {"head": x.head, "tail": x.tail}, relation),
                         map(lambda x: {"head": x.head, "relation": x.relation, "tail": x.tail}, relation))
        basic_update = partial(self.update, "basic")
        with ThreadPoolExecutor(max_workers=8) as executor:
            executor.map(basic_update,
                         map(lambda x: {"code": x["code"]}, data),
                         data)

    def sina_concept(self):
        stock = self.db.get_collection("basic").find({}, {"_id": 0, "code": 1})

        def run_one(code: str):
            time.sleep(random.randint(3, 8))
            entity, relation = sina.get_concept(code)
            for x in entity:
                self.update("entity",
                            {"name": x.name},
                            {"name": x.name, "type": x.type})
            for x in relation:
                self.update("relation",
                            {"head": x.head, "tail": x.tail},
                            {"head": x.head, "relation": x.relation, "tail": x.tail})

        with ThreadPoolExecutor(max_workers=3) as executor:
            executor.map(run_one,
                         list(map(lambda x: x["code"], stock)))

    def sina_holder(self):
        stock = self.db.get_collection("basic").find({}, {"_id": 0, "code": 1})

        def run_one(code: str):
            time.sleep(random.randint(3, 8))
            entity, relation = sina.get_holder(code)
            for x in entity:
                self.update("entity",
                            {"name": x.name},
                            {"name": x.name, "type": x.type})
            for x in relation:
                self.update("relation",
                            {"head": x.head, "tail": x.tail},
                            {"head": x.head, "relation": x.relation, "tail": x.tail, "extend": x.extend})

        with ThreadPoolExecutor(max_workers=3) as executor:
            executor.map(run_one,
                         list(map(lambda x: x["code"], stock)))

    def jrj_product(self):
        stock = self.db.get_collection("basic").find({}, {"_id": 0, "code": 1})

        def run_one(code: str):
            entity, relation = jrj.get_product(code)
            for x in entity:
                self.update("entity",
                            {"name": x.name},
                            {"name": x.name, "type": x.type})
            for x in relation:
                self.update("relation",
                            {"head": x.head, "tail": x.tail},
                            {"head": x.head, "relation": x.relation, "tail": x.tail, "extend": x.extend})

        with ThreadPoolExecutor(max_workers=16) as executor:
            executor.map(run_one,
                         list(map(lambda x: x["code"], stock)))

    def jrj_holder(self):
        stock = self.db.get_collection("basic").find({}, {"_id": 0, "code": 1})

        def run_one(code: str):
            entity, relation = jrj.get_holder(code)
            for x in entity:
                self.update("entity",
                            {"name": x.name},
                            {"name": x.name, "type": x.type})
            for x in relation:
                self.update("relation",
                            {"head": x.head, "tail": x.tail},
                            {"head": x.head, "relation": x.relation, "tail": x.tail, "extend": x.extend})

        with ThreadPoolExecutor(max_workers=16) as executor:
            executor.map(run_one,
                         list(map(lambda x: x["code"], stock)))

    def jrj_report_topic(self):
        stock = self.db.get_collection("basic").find({}, {"_id": 0, "code": 1})

        def run_one(code: str):
            entity, relation, topic = jrj.get_report_topic(code)
            for x in entity:
                self.update("entity",
                            {"name": x.name},
                            {"name": x.name, "type": x.type})
            for x in relation:
                self.update("relation",
                            {"head": x.head, "tail": x.tail},
                            {"head": x.head, "relation": x.relation, "tail": x.tail})
            for x in topic:
                self.update("article",
                            {"url": x["url"]},
                            x)

        with ThreadPoolExecutor(max_workers=16) as executor:
            executor.map(run_one,
                         list(map(lambda x: x["code"], stock)))

    def jrj_report_content(self, num: int):
        url = self.db.get_collection("article").find({"source": "jrj", "type": "report", "content": {"$exists": False}},
                                                     {"_id": 0, "url": 1}).limit(num)

        def run_one(url_: str):
            data = jrj.get_report_content(url_)
            self.update("article",
                        {"url": url_},
                        data,
                        False)

        with ThreadPoolExecutor(max_workers=16) as executor:
            executor.map(run_one,
                         list(map(lambda x: x["url"], url)))

    @staticmethod
    def run_tushare_basic():
        kg = Basic_kg()
        kg.tushare_basic()

    @staticmethod
    def run_sina_concept():
        kg = Basic_kg()
        kg.sina_concept()

    @staticmethod
    def run_sina_holder():
        kg = Basic_kg()
        kg.sina_holder()

    @staticmethod
    def run_jrj_product():
        kg = Basic_kg()
        kg.jrj_product()

    @staticmethod
    def run_jrj_holder():
        kg = Basic_kg()
        kg.jrj_holder()

    @staticmethod
    def run_jrj_report_topic():
        kg = Basic_kg()
        kg.jrj_report_topic()

    @staticmethod
    def run_jrj_report_content(num: int):
        kg = Basic_kg()
        kg.jrj_report_content(num)


if __name__ == '__main__':
    Basic_kg.run_jrj_report_topic()
