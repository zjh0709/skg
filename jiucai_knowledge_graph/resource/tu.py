import tushare as ts
import datetime
from collections import namedtuple


def get_stock_basic() -> list:
    nodes, links = [], []
    Node = namedtuple("Node", "name type source")
    Link = namedtuple("Link", "head link tail source")
    df = ts.get_stock_basics()
    df.reset_index(inplace=True)
    data = df.to_dict(orient="records")
    industry_mapper = ts.get_industry_classified().groupby(["code"])["c_name"].apply(lambda x: x.tolist()).to_dict()
    concept_mapper = ts.get_concept_classified().groupby(["code"])["c_name"].apply(lambda x: x.tolist()).to_dict()
    area_mapper = ts.get_area_classified().groupby(["code"])["area"].apply(lambda x: x.tolist()).to_dict()
    for d in data:
        nodes.append(Node(d["name"], "公司", "tu"))
        links.append(Link(d["code"], "等于", d["name"], "tu"))
        for k in set(industry_mapper.get(d["code"], []) + [d["industry"]]):
            nodes.append(Node(k, "行业", "tu"))
            links.append(Link(d["code"], "属于", k, "tu"))
        for k in concept_mapper.get(d["code"], []):
            nodes.append(Node(k, "概念", "tu"))
            links.append(Link(d["code"], "属于", k, "tu"))
        for k in set(area_mapper.get(d["code"], []) + [d["area"]]):
            nodes.append(Node(k, "区域", "tu"))
            links.append(Link(d["code"], "属于", k, "tu"))
        del d["industry"], d["area"]
    nodes = list(set(nodes))
    return nodes, links, data


def get_news_topic(num: int = 1000) -> list:
    df = ts.get_latest_news(top=num, show_content=False)
    df["timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %X")
    df["source"] = "tu"
    data = df.to_dict(orient="records")
    return data


def get_news_content(url: str) -> str:
    content = ts.latest_content(url)
    content = content if content is not None else "--"
    return content


if __name__ == '__main__':
    print(get_news_content('http://finance.sina.com.cn/stock/marketresearch/2018-06-14/doc-ihcwpcmr0207793.shtml'))