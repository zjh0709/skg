from jiucai_knowledge_graph import ZK_HOST
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.protocol.states import KeeperState


class ZkUtil(object):
    def __init__(self):
        self.zk = KazooClient(hosts=ZK_HOST)
        try:
            self.zk.start()
        except KazooTimeoutError as e:
            e.__traceback__

    def exists(self, path: str):
        if self.zk.client_state == KeeperState.CONNECTED:
            return self.zk.exists(path)
        else:
            return None

    def get_value(self, path: str):
        if self.zk.client_state == KeeperState.CONNECTED:
            return self.zk.get(path)[0].decode()
        else:
            return None

    def create(self, path: str, value):
        if isinstance(value, int):
            value = str(value)
        if self.zk.client_state == KeeperState.CONNECTED:
            if self.zk.exists(path):
                self.zk.delete(path, recursive=True)
            self.zk.create(path=path, value=value.encode(), makepath=True)

    def create_ephemeral(self, path: str, value):
        if isinstance(value, int):
            value = str(value)
        if self.zk.client_state == KeeperState.CONNECTED:
            self.zk.create(path=path, value=value.encode(), ephemeral=True, makepath=True)

    def delete(self, path):
        self.zk.delete(path, recursive=True)

    def counter(self, path):
        if self.zk.client_state == KeeperState.CONNECTED:
            if self.zk.exists(path):
                self.zk.delete(path, recursive=True)
            return self.zk.Counter(path)
        else:
            return 0

    def stop(self):
        try:
            self.zk.close()
        except Exception as e:
            e.__traceback__

    def client(self):
        return self.zk

