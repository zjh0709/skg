from kazoo.client import KazooClient
from kazoo.recipe.counter import Counter


zk = KazooClient(hosts="master")


if __name__ == '__main__':
    Counter.client