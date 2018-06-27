import requests
from bs4 import BeautifulSoup
import re
from collections import namedtuple
import json


def get_chain_topic() -> tuple:
    nodes, links, data = [], [], []
    Node = namedtuple("Node", "name type source")
    Link = namedtuple("Link", "head link tail source")
    index_url = "http://industry.hexun.com/index.aspx"
    industry_url = "http://industry.hexun.com/data/jsondata/leftNav.ashx?industry={}&type=0"
    headers = {
        "User-Agent": "Mozilla/4.0 (compatible; MSIE7.0; WindowsNT5.1; Maxthon2.0)"
    }
    r = requests.get(index_url, headers=headers)
    r.encoding = "gbk"
    soup = BeautifulSoup(r.text, "html.parser")
    for dt in soup.find_all("dt", type="0"):
        industry = dt.text
        nodes.append(Node(industry, "行业", "hexun"))
        r = requests.get(industry_url.format(dt.attrs["id"]))
        r.encoding = "utf-8"
        s = r.text.strip().lstrip("hxbase_json1(").rstrip(")")
        j = json.loads(s.replace("result", "\"result\""))
        for d in j.get("result", []):
            product_name = d["text"]
            nodes.append(Node(product_name, "产品", "hexun"))
            links.append(Link(product_name, "属于", industry, "hexun"))
            data.append({"product": product_name, "href": d["href"]})
    nodes = list(set(nodes))
    links = list(set(links))
    return nodes, links, data


def get_chain_content(d: dict) -> tuple:
    nodes, links = [], []
    Node = namedtuple("Node", "name type source")
    Link = namedtuple("Link", "head link tail source")
    upstream, downstream = [], []
    url = "http://industry.hexun.com/{}"
    headers = {
        "User-Agent": "Mozilla/4.0 (compatible; MSIE7.0; WindowsNT5.1; Maxthon2.0)"
    }
    r = requests.get(url.format(d["href"]), headers=headers)
    r.encoding = "gbk"
    soup = BeautifulSoup(r.text, "html.parser")
    left = soup.find(class_="sidebar_left")
    if left:
        a = left.find_all("a")
        if a:
            upstream = [d.text for d in a if d.text != "无"]
        for p in upstream:
            nodes.append(Node(p, "产品", "hexun"))
            links.append(Link(d["product"], "上游", p, "hexun"))
            links.append(Link(p, "下游", d["product"], "hexun"))
    right = soup.find(class_="sidebar_right")
    if right:
        a = right.find_all("a")
        if a:
            downstream = [d.text for d in a if d.text != "无"]
        for p in downstream:
            nodes.append(Node(p, "产品", "hexun"))
            links.append(Link(d["product"], "下游", p, "hexun"))
            links.append(Link(p, "上游", d["product"], "hexun"))
    more = soup.find("p", class_="more_info")
    if more:
        a, href, patt = more.find("a"), None, None
        if a:
            href = a.attrs["href"]
        if href:
            patt = re.search("(?<=s)(.*)+(?=.shtml)", href)
        if patt and len(patt.group().split("_"))==2:
            parm1, parm2 = patt.group().split("_")
            r2 = requests.get("http://industry.hexun.com/data/jsondata/POrVstocks.ashx?" +
                              "pid={}&type={}&pageIndex=1&count=200".format(parm1, parm2))
            s2 = r2.text.strip().lstrip("(").rstrip(")")
            j2 = json.loads(s2)
            for jj in j2.get("result", []):
                if jj.get("mycode"):
                    links.append(Link(jj["mycode"], "生产", d["product"], "hexun"))
    return nodes, links


def get_info(code: str) -> dict:
    nodes, links, info = [], [], {}
    Node = namedtuple("Node", "name type source")
    Link = namedtuple("Link", "head relation tail source")
    url = "http://stockdata.stock.hexun.com/gszl/s{}.shtml".format(code)
    headers = {
        "User-Agent": "Mozilla/4.0 (compatible; MSIE7.0; WindowsNT5.1; Maxthon2.0)"
    }
    r = requests.get(url, headers=headers)
    r.encoding = "gbk"
    soup = BeautifulSoup(r.text, "html.parser")
    try:
        for tr in soup.find_all("tr"):
            td = tr.find_all("td")
            if td and len(td) == 2:
                info.setdefault(td[0].text.strip(), td[1].text.strip())
        if "所属行业" in info:
            for d in info.get("所属行业", "").split("、"):
                nodes.append(Node(d, "行业", "hexun"))
                links.append(Link(code, "属于", d, "hexun"))
            del info["所属行业"]
        if "所属概念" in info:
            for d in info.get("所属概念", "").split("、"):
                nodes.append(Node(d, "概念", "hexun"))
                links.append(Link(code, "属于", d, "hexun"))
            del info["所属概念"]
    except TypeError as e:
        e.__traceback__
    return nodes, links, info


if __name__ == '__main__':
    print(get_chain_content({"product": "锦纶长丝", "href": "c204_54.shtml"}))
