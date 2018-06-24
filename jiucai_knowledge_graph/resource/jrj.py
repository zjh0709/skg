import requests
from bs4 import BeautifulSoup
import re
import logging
from collections import namedtuple
import json
import time


def __recognize_entity(entity: str):
    if len(entity) < 5:
        return "个人"
    elif re.search("基金|社保|保险", entity):
        return "基金"
    else:
        return "公司"


def get_news_topic(code: str, page: int = 1) -> tuple:
    if page == 1:
        topic_url = "http://stock.jrj.com.cn/share,{},ggxw.shtml".format(code)
    else:
        topic_url = "http://stock.jrj.com.cn/share,{},ggxw_{}.shtml".format(code, page)
    url_expr = re.compile("http://stock.jrj.com.cn/\d{4}/\d{2}/\d+.shtml")
    r = requests.get(topic_url)
    r.encoding = "gbk"
    soup = BeautifulSoup(r.text, "html.parser")
    links_div = soup.find_all("a", href=url_expr)
    links = []
    for link in links_div:
        link = {"url": link.get("href"),
                "title": link.text,
                "code": code,
                "source": "jrj",
                "type": "news"}
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
    r.encoding = "gbk"
    ret = {"url": url}
    try:
        soup = BeautifulSoup(r.text, "html.parser")
        content_div = soup.find("div", class_="texttit_m1")
        content = "--"
        for d in content_div.find_all(recursive=False):
            if "class" not in d.attrs:
                content += d.text.strip()
        if content != "":
            ret.setdefault("content", content)
    except Exception as e:
        e.__traceback__
        ret.setdefault("content", "--")
    return ret


def get_report_topic(code: str) -> tuple:
    nodes, links, topics = [], [], []
    Node = namedtuple("Node", "name type source")
    Link = namedtuple("Link", "head link tail source")
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
            topics.append({"declare_date": d[0], "code": d[1], "name": d[2],
                          "title": d[5], "analyst": d[6], "url": url_format.format(d[7]),
                          "cs_name": d[9], "source": "jrj", "type": "report"})
            nodes.append(Node(d[15], "行业", "jrj"))
            if isinstance(d[16], list):
                for c, n in d[16]:
                    nodes.append(Node(n, "概念", "jrj"))
                    links.append(Link(code, "属于", n, "jrj"))
    except Exception as e:
        logging.warning(e)
    nodes = list(set(nodes))
    links = list(set(links))
    return nodes, links, topics


def get_report_content(url: str) -> dict:
    r = requests.get(url)
    r.encoding = "gb2312"
    ret = {"url": url}
    try:
        soup = BeautifulSoup(r.text, "html.parser")
        replayContent_div = soup.find("div", id="replayContent")
        if replayContent_div:
            div = replayContent_div.find_all("div", limit=1)
            if div:
                ret.setdefault("content", div[0].text.strip())
    except Exception as e:
        e.__traceback__
        ret.setdefault("content", "--")
    return ret


def get_product(code: str):
    url = "http://stock.jrj.com.cn/share,{},zyyw.shtml".format(code)
    nodes, links = [], []
    Node = namedtuple("Node", "name type source")
    Link = namedtuple("Link", "head link tail extend source")
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
                    extend_ = json.dumps({"主营收入": td[1].text,
                                          "收入比例": td[2].text,
                                          "主营成本": td[3].text,
                                          "成本比例": td[4].text,
                                          "利润比例": td[5].text,
                                          "毛利率": td[6].text})
                    nodes.append(Node(product_, "产品", "jrj"))
                    links.append(Link(code, "生产", product_, extend_, "jrj"))
    except Exception as e:
        logging.warning(e)
    return nodes, links


def get_holder(code: str):
    url = "http://stock.jrj.com.cn/share,{},sdgd.shtml".format(code)
    nodes, links = [], []
    Node = namedtuple("Node", "name type source")
    Link = namedtuple("Link", "head link tail extend source")
    r = requests.get(url)
    r.encoding = "gbk"
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
                        extend_ = json.dumps({"持股数量": td[2].text,
                                              "持股比例": td[3].text,
                                              "变化": td[4].text,
                                              "股份类型": td[5].text,
                                              "股东性质": td[6].text})
                        nodes.append(Node(td[1].text, __recognize_entity(td[1].text), "jrj"))
                        links.append(Link(code, "股东", company_,  extend_, "jrj"))
            else:
                continue
    except Exception as e:
        logging.warning(e)
    return nodes, links


if __name__ == '__main__':
    print(get_holder("600597"))
