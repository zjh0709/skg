from argparse import ArgumentParser
import datetime
import logging
import sys
from jiucai.job.basic import BasicJob


if __name__ == "__main__":
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    parser = ArgumentParser(usage="%s main.py" % sys.executable,
                            description="run main.",
                            epilog="action source num date")
    parser.add_argument("--action", dest="action", help="job")
    parser.add_argument("--source", dest="source", help="from")
    parser.add_argument("--num", dest="num", type=int, help="number")
    parser.add_argument("--date", dest="date", help="date", default=today)
    args = parser.parse_args()

    logging.info("mission start.")
    if args.action == "basic" and args.source == "tu":
        BasicJob.run("tu_basic")
    elif args.action == "concept" and args.source == "sina":
        BasicJob.run("sina_concept")
    elif args.action == "holder" and args.source == "jrj":
        BasicJob.run("jrj_holder")
    elif args.action == "product" and args.source == "jrj":
        BasicJob.run("jrj_product")
    elif args.action == "chain" and args.source == "hexun":
        BasicJob.run("hexun_chain")
    elif args.action == "report_topic" and args.source == "jrj":
        BasicJob.run("jrj_report_topic")
    elif args.action == "report_content" and args.source == "jrj":
        BasicJob.run("jrj_report_content", num=100 if args.num is None else args.num)
    elif args.action == "news_topic" and args.source == "jrj":
        BasicJob.run("jrj_news_topic", recover=False)
    elif args.action == "news_content" and args.source == "jrj":
        BasicJob.run("jrj_news_content", num=100 if args.num is None else args.num)
    elif args.action == "news_topic" and args.source == "tu":
        BasicJob.run("tu_news_topic", num=100 if args.num is None else args.num)
    elif args.action == "news_content" and args.source == "tu":
        BasicJob.run("tu_news_content", num=100 if args.num is None else args.num)
    elif args.action == "report_topic" and args.source == "sina":
        BasicJob.run("sina_report_topic", recover=True)
    elif args.action == "report_content" and args.source == "sina":
        BasicJob.run("sina_report_content", num=100 if args.num is None else args.num)

    logging.info("mission complete.")
