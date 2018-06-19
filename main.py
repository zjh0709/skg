import argparse
import datetime
import logging
from kg_job.Basic_kg import Basic_kg


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
        Basic_kg.run_tushare_basic()
    elif args.action == "concept" and args.source == "sina":
        Basic_kg.run_sina_concept()
    elif args.action == "holder" and args.source == "jrj":
        Basic_kg.run_jrj_holder()
    elif args.action == "product" and args.source == "jrj":
        Basic_kg.run_jrj_product()
    elif args.action == "report-topic" and args.source == "jrj":
        Basic_kg.run_jrj_report_topic()
    elif args.action == "report-content" and args.source == "jrj":
        Basic_kg.run_jrj_report_content(int(args.num) if args.num is not None else 20)
    elif args.action == "news-topic" and args.source == "jrj":
        Basic_kg.run_jrj_news_topic(args.num is not None)
    elif args.action == "news-content" and args.source == "jrj":
        Basic_kg.run_jrj_news_content(int(args.num) if args.num is not None else 20)

    logging.info("mission complete.")
