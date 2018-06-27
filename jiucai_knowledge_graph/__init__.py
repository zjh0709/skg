import logging

MONGODB_HOST = "master"
MONGODB_PORT = 17585
MONGODB_DB = "skg"
ZK_HOST = "master:2181"
ZK_ROOT = "/skg/"

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')
