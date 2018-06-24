import logging
from collections import namedtuple

import pymongo
import time

from jiucai_knowledge_graph import MONGODB_HOST, MONGODB_PORT, MONGODB_DB


class DataUtil(object):
    def __init__(self):
        self.client = pymongo.MongoClient(host=MONGODB_HOST, port=MONGODB_PORT, connect=False)
        self.db = self.client.get_database(MONGODB_DB)

    def get_stocks(self) -> list:
        stocks = self.db.get_collection("info").find({}, {"_id": 0, "code": 1})
        stocks = list(map(lambda x: x["code"], stocks))
        return stocks

    def get_articles(self, where: dict, filed: dict, limit: int):
        urls = self.db.get_collection("article").find(where, filed).limit(limit)
        return list(urls)

    def update(self, collection_name: str, spec: dict, document: dict, upsert=True) -> None:
        document.setdefault("timestamp", int(time.time() * 1000))
        self.db.get_collection(collection_name).update(spec, {"$set": document}, upsert)
        logging.info(spec)

    def save_node(self, node: namedtuple) -> None:
        # noinspection PyProtectedMember
        self.update("node",
                    {"name": node.name, "source": node.source},
                    node._asdict(),
                    True)

    def save_link(self, link: namedtuple) -> None:
        # noinspection PyProtectedMember
        self.update("link",
                    {"head": link.head, "tail": link.tail},
                    link._asdict(),
                    True)

    def save_info(self, data: dict):
        # noinspection PyProtectedMember
        self.update("info",
                    {"code": data["code"]},
                    data,
                    True)

    def save_article(self, article: dict):
        # noinspection PyProtectedMember
        self.update("article",
                    {"url": article["url"]},
                    article,
                    True)

