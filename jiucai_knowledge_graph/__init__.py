import logging

MONGODB_HOST = "192.168.1.18"
MONGODB_PORT = 17585
MONGODB_DB = "skg"
ZK_HOST = "192.168.1.18:2181"
ZK_ROOT = "/skg/"

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')
