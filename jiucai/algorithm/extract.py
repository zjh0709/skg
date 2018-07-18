from jiucai.util.DataUtil import DataUtil
import jieba
import io


def dictionary_file():
    data_util = DataUtil()
    cursor = data_util.load_node(field={"_id": 0, "name": 1},
                                 limit=1000)
    s = ""
    for d in cursor:
        s += "{} 99\n".format(d["name"])
    f = io.StringIO(s)
    return f


def keyword():
    data_util = DataUtil()
    cursor = data_util.load_article(where={"content": {"$exists": True, "$ne": "--"}},
                                    field={"_id": 0, "code": 1, "title": 1, "content": 1},
                                    limit=200)
    # jieba.enable_parallel(8)
    f = dictionary_file()
    jieba.load_userdict(f)
    data = [(list(jieba.cut(d.get("title", "").replace(" ", ""))),
             list(jieba.cut(d["content"].replace(" ", ""))),
             d.get("code")) for d in cursor]
    print(data)


if __name__ == '__main__':
    keyword()