import argparse
import datetime
import logging
from jiucai_knowledge_graph.job.basic import BasicJob


if __name__ == "__main__":
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    ap = argparse.ArgumentParser()
    ap.add_argument("-a", "--action", help="", required=True)
    ap.add_argument("-s", "--source", help="report", required=False)
    ap.add_argument("-d", "--date", help=today, required=False, default=today)
    ap.add_argument("-n", "--num", help="", required=False)
    args = ap.parse_args()
    logging.info("mission start.")
    if args.action == "basic" and args.source == "tushare":
        BasicJob.run_tushare_basic()
    elif args.action == "concept" and args.source == "sina":
        BasicJob.run_sina_concept()
    elif args.action == "holder" and args.source == "jrj":
        BasicJob.run_jrj_holder()
    elif args.action == "product" and args.source == "jrj":
        BasicJob.run_jrj_product()
    elif args.action == "chain" and args.source == "hexun":
        BasicJob.run_hexun_chain()
    elif args.action == "report_topic" and args.source == "jrj":
        BasicJob.run_jrj_report_topic()
    elif args.action == "report_content" and args.source == "jrj":
        BasicJob.run_jrj_report_content()
    elif args.action == "news_topic" and args.source == "jrj":
        BasicJob.run_jrj_news_topic()
    elif args.action == "news_content" and args.source == "jrj":
        BasicJob.run_jrj_news_content()
    elif args.action == "news_topic" and args.source == "tushare":
        # noinspection PyCallByClass
        BasicJob.tushare_news_topic()
    elif args.action == "news_content" and args.source == "tushare":
        # noinspection PyCallByClass
        BasicJob.tushare_news_content()

    logging.info("mission complete.")
