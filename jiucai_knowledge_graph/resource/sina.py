import requests
from bs4 import BeautifulSoup
import re
import logging
from collections import namedtuple


def get_report_topic(code: str, page: int = 1) -> tuple:
    topic_url = "http://vip.stock.finance.sina.com.cn/q/go.php/vReport_List/kind/search/index.phtml?symbol={" \
                "}&t1=all&p={}".format(code, page)
    url_expr = re.compile("vip.stock.finance.sina.com.cn/q/go.php/vReport_Show/kind/search/rptid")
    headers = {
        "User-Agent": "Mozilla/4.0 (compatible; MSIE7.0; WindowsNT5.1; Maxthon2.0)"
    }
    r = requests.get(topic_url, headers=headers)
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
    headers = {
        "User-Agent": "Mozilla/4.0 (compatible; MSIE7.0; WindowsNT5.1; Maxthon2.0)"
    }
    r = requests.get(url, headers=headers)
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


def get_concept(code: str) -> list:
    url = "http://vip.stock.finance.sina.com.cn/corp/go.php/vCI_CorpOtherInfo/stockid/{}/menu_num/2.phtml".format(code)
    nodes, links = [], []
    Node = namedtuple("Node", "name type source")
    Link = namedtuple("Link", "head link tail source")
    headers = {
        "User-Agent": "Mozilla/4.0 (compatible; MSIE7.0; WindowsNT5.1; Maxthon2.0)"
    }
    r = requests.get(url, headers=headers)
    r.encoding = "gbk"
    try:
        soup = BeautifulSoup(r.text, "html.parser")
        tr = soup.find_all("tr")
        for tr_ in tr:
            td = tr_.find_all("td")
            if len(td) == 2 and "href" in td[1].prettify():
                concept_ = td[0].text
                nodes.append(Node(concept_, "概念", "sina"))
                links.append(Link(code, "属于", concept_, "sina"))
    except Exception as e:
        logging.warning(e)
    return nodes, links


if __name__ == '__main__':
    print(get_concept("300345"))
