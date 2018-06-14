import requests
from bs4 import BeautifulSoup
import re
import logging
from collections import namedtuple
import json
import time


def get_news_topic(code: str, page: int = 1) -> tuple:
    if page == 1:
        topic_url = "http://stock.jrj.com.cn/share,{},ggxw.shtml".format(code)
    else:
        topic_url = "http://stock.jrj.com.cn/share,{},ggxw_{}.shtml".format(code, page)
    url_expr = re.compile("http://stock.jrj.com.cn/\d{4}/\d{2}/\d+.shtml")
    r = requests.get(topic_url)
    r.encoding = "gb2312"
    soup = BeautifulSoup(r.text, "html.parser")
    links_div = soup.find_all("a", href=url_expr)
    links = []
    for link in links_div:
        link = {"url": link.get("href"),
                "title": link.text,
                "code": code,
                "source": "jrj"}
        links.append(link)
    page_buttons = soup.find_all("a", href=re.compile("ggxw_\d+.shtml"))
    max_page = 1
    patt = re.compile("(?<=ggxw_)\d+(?=.shtml)")
    # noinspection PyBroadException
    try:
        if page_buttons:
            max_page = max(map(int,
                               [patt.search(d.attrs.get("href", "ggxw_1.shtml")).group() for
                                d in page_buttons]))
    except Exception as e:
        logging.warning(e)
        pass
    return links, max_page


def get_news_content(url: str) -> dict:
    r = requests.get(url)
    r.encoding = "gb2312"
    ret = {}
    try:
        soup = BeautifulSoup(r.text, "html.parser")
        content_div = soup.find("div", class_="texttit_m1")
        if content_div:
            ret.setdefault("content", content_div.text.strip())
    except Exception as e:
        logging.warning(e)
    return ret


def get_report_topic(code: str) -> tuple:
    entity, relation, topic = [], [], []
    Entity = namedtuple("Entity", "name type")
    Relation = namedtuple("Relation", "head relation tail")
    topic_url = "http://stock.jrj.com.cn/action/yanbao/getJiGouGongSiYanBao.jspa?dateInterval=3650&stockCode={}" \
                "&pn={}&ps={}&_dc={}"
    url_format = "http://istock.jrj.com.cn/article,yanbao,{}.html"
    r = requests.get(topic_url.format(code, 1, 1, int(time.time() * 1000)))
    r.encoding = "utf-8"
    try:
        data = json.loads(r.text.strip().lstrip("var yanbaolist=").rstrip(";"))
        total = data.get("summary", {}).get("total", 1)
        r = requests.get(topic_url.format(code, 1, total, int(time.time() * 1000)))
        r.encoding = "utf-8"
        data = json.loads(r.text.strip().lstrip("var yanbaolist=").rstrip(";"))
        for d in data["data"]:
            topic.append({"declare_date": d[0], "code": d[1], "name": d[2],
                          "title": d[5], "analyst": d[6], "url": url_format.format(d[7]),
                          "cs_name": d[9], "source": "jrj", "type": "report"})
            entity.append(Entity(d[15], "行业"))
            if isinstance(d[16], list):
                for c, n in d[16]:
                    entity.append(Entity(n, "概念"))
                    relation.append(Relation(code, "属于", n))

    except Exception as e:
        logging.warning(e)

    return list(set(entity)), list(set(relation)), topic


def get_report_content(url: str) -> dict:
    r = requests.get(url)
    r.encoding = "gb2312"
    ret = {}
    try:
        soup = BeautifulSoup(r.text, "html.parser")
        replayContent_div = soup.find("div", id="replayContent")
        if replayContent_div:
            div = replayContent_div.find_all("div", limit=1)
            if div:
                ret.setdefault("content", div[0].text.strip())
    except Exception as e:
        logging.warning(e)
    return ret


def get_product(code: str):
    url = "http://stock.jrj.com.cn/share,{},zyyw.shtml".format(code)
    entity, relation = [], []
    Entity = namedtuple("Entity", "name type")
    Relation = namedtuple("Relation", "head relation tail extend")
    r = requests.get(url)
    r.encoding = "gb2312"
    try:
        soup = BeautifulSoup(r.text, "html.parser")
        div = soup.find("div", class_="tabs2")
        tr = div.find_all("tr")
        start_flag = False
        for tr_ in tr:
            if "按产品分类" not in tr_.text and start_flag is False or ("按行业分类" in tr_.text):
                continue
            elif start_flag is False:
                start_flag = True
                continue
            elif "分类" in tr_.text and start_flag is True:
                break
            else:
                td = tr_.find_all("td")
                if len(td) == 7:
                    product_ = td[0].text
                    extend_ = json.dumps({"percent": td[2].text, "rate": td[-1].text})
                    entity.append(Entity(product_, "产品"))
                    relation.append(Relation(code, "生产", product_, extend_))
    except Exception as e:
        logging.warning(e)
    return entity, relation


def get_holder(code: str):
    url = "http://stock.jrj.com.cn/share,{},sdgd.shtml".format(code)
    entity, relation = [], []
    Entity = namedtuple("Entity", "name type")
    Relation = namedtuple("Relation", "head relation tail extend")
    r = requests.get(url)
    r.encoding = "gb2312"
    try:
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find_all("table")
        for table_ in table:
            th = table_.find_all("th")
            if th and len(th) == 7:
                tr = table_.find_all("tr")
                for tr_ in tr:
                    td = tr_.find_all("td")
                    if td and len(td) == 7:
                        company_ = td[1].text
                        extend_ = json.dumps({"percent": td[3].text})
                        entity.append(Entity(td[1].text, "公司/基金/个人"))
                        relation.append(Relation(code, "股东", company_, extend_))
            else:
                continue
    except Exception as e:
        logging.warning(e)
    return entity, relation


if __name__ == '__main__':
    print(get_report_content("http://istock.jrj.com.cn/article,yanbao,29871320.html"))
