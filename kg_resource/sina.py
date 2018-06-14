import requests
from bs4 import BeautifulSoup
import re
import logging
from collections import namedtuple
import json


def get_report_topic(code: str, page: int = 1) -> tuple:
    topic_url = "http://vip.stock.finance.sina.com.cn/q/go.php/vReport_List/kind/search/index.phtml?symbol={" \
                "}&t1=all&p={}".format(code, page)
    url_expr = re.compile("vip.stock.finance.sina.com.cn/q/go.php/vReport_Show/kind/search/rptid")
    r = requests.get(topic_url)
    r.encoding = "gb2312"
    soup = BeautifulSoup(r.text, "html.parser")
    links_div = soup.find_all("a", href=url_expr)
    links = []
    for link in links_div:
        link = {"url": link.get("href"),
                "title": link.get("title"),
                "code": code,
                "source": "sina"}
        links.append(link)
    page_buttons = soup.find_all("a", onclick=re.compile("set_page_num"))
    max_page = 1
    if page_buttons:
        last_onclick = page_buttons[-1].get("onclick")
        re_page_num = re.compile("(?<=set_page_num\(')(.*)?(?='\))")
        re_result = re_page_num.search(last_onclick)
        if re_result:
            max_page = int(re_result.group())
    return links, max_page


def get_report_content(url: str) -> dict:
    r = requests.get(url)
    r.encoding = "gb2312"
    ret = {}
    try:
        soup = BeautifulSoup(r.text, "html.parser")
        content_select = soup.find("div", class_="content")
        if content_select:
            title_select = content_select.find("h1")
            if title_select:
                ret.setdefault("title", title_select.text)
            creab_select = content_select.find("div", class_="creab")
            if creab_select:
                ret.setdefault("span", [e.text for
                                        e in creab_select.findAll("span")])
            document_select = content_select.find("div", class_="blk_container")
            if document_select:
                ret.setdefault("content", document_select.text.strip())
    except Exception as e:
        logging.warning(e)
    return ret


# return entity, relation
def get_concept(code: str) -> list:
    url = "http://vip.stock.finance.sina.com.cn/corp/go.php/vCI_CorpOtherInfo/stockid/{}/menu_num/2.phtml".format(code)
    entity, relation = [], []
    Entity = namedtuple("Entity", "name type")
    Relation = namedtuple("Relation", "head relation tail")
    r = requests.get(url)
    r.encoding = "gb2312"
    try:
        soup = BeautifulSoup(r.text, "html.parser")
        tr = soup.find_all("tr")
        for tr_ in tr:
            td = tr_.find_all("td")
            if len(td) == 2 and "href" in td[1].prettify():
                concept_ = td[0].text
                entity.append(Entity(concept_, "概念"))
                relation.append(Relation(code, "属于", concept_))
    except Exception as e:
        logging.warning(e)
    return entity, relation


def get_holder(code: str):
    url = "http://vip.stock.finance.sina.com.cn/corp/go.php/vCI_StockHolder/stockid/{}/displaytype/30.phtml".format(
        code)
    entity, relation = [], []
    Entity = namedtuple("Entity", "name type")
    Relation = namedtuple("Relation", "head relation tail extend")
    r = requests.get(url)
    r.encoding = "gb2312"
    try:
        soup = BeautifulSoup(r.text, "html.parser")
        tr = soup.find_all("tr")
        start_flag = False
        for tr_ in tr:
            if "编号" not in tr_.text and start_flag is False:
                continue
            elif start_flag is False:
                start_flag = True
                continue
            else:
                td = tr_.find_all("td")
                if len(td) == 5:
                    company_ = td[1].text
                    extend_ = json.dumps({"percent": td[3].text, "property": td[4].text})
                    entity.append(Entity(company_, "公司/基金/个人"))
                    relation.append(Relation(code, "股东", company_, extend_))
                else:
                    break
    except Exception as e:
        logging.warning(e)
    return entity, relation


if __name__ == '__main__':
    print(get_concept("300345"))
