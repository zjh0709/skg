import tushare as ts
import time
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor


# return entity(dict), relation(tuple), basic(dict)
def get_stock_basic() -> list:
    entity, relation, basic = [], [], []
    Entity = namedtuple("Entity", "name type")
    Relation = namedtuple("Relation", "head relation tail")
    df = ts.get_stock_basics()
    df.reset_index(inplace=True)
    data = df.to_dict(orient="records")
    industry_mapper = ts.get_industry_classified().groupby(["code"])["c_name"].apply(lambda x: x.tolist()).to_dict()
    concept_mapper = ts.get_concept_classified().groupby(["code"])["c_name"].apply(lambda x: x.tolist()).to_dict()
    area_mapper = ts.get_area_classified().groupby(["code"])["area"].apply(lambda x: x.tolist()).to_dict()
    for d in data:
        entity.append(Entity(d["name"], "公司名称"))
        relation.append(Relation(d["code"], "等于", d["name"]))
        for k in set(industry_mapper.get(d["code"], []) + [d["industry"]]):
            entity.append(Entity(k, "行业"))
            relation.append(Relation(d["code"], "属于", k))
        for k in concept_mapper.get(d["code"], []):
            entity.append(Entity(k, "概念"))
            relation.append(Relation(d["code"], "属于", k))
        for k in set(area_mapper.get(d["code"], []) + [d["area"]]):
            entity.append(Entity(k, "区域"))
            relation.append(Relation(d["code"], "属于", k))
        del d["industry"], d["area"]
    return list(set(entity)), relation, data


def get_news_topic(num: int = 1000) -> list:
    df = ts.get_latest_news(top=num, show_content=False)
    df['timestamp'] = int(time.time())
    data = df.to_dict(orient="records")
    return data


def get_news_content(url: str):
    content = ts.latest_content(url)
    content = content if content is not None else "--"
    return content


if __name__ == '__main__':
    print(get_news_content('http://finance.sina.com.cn/stock/marketresearch/2018-06-14/doc-ihcwpcmr0207793.shtml'))