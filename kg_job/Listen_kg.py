from kg_job import MONGODB_HOST, MONGODB_PORT, MONGODB_DB, ZK_HOST, ZK_ROOT
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError


class Listen_kg(object):
    def __init__(self):
        self.zk = KazooClient(hosts=ZK_HOST)
        self.zk_start()

    def zk_start(self):
        try:
            self.zk.start()
        except KazooTimeoutError as e:
            self.zk = None
            e.__traceback__


zk = KazooClient(hosts=ZK_HOST)
zk.start()


def myWatch(zk_, type_, state_, path_):
    print("type {} state {} path {}".format(str(type_), str(state_), str(path_)))
    return "aaaa"


data = zk.get("/mytest", myWatch)
print(data)

input()
