from __future__ import annotations

from src.utils.config import load_config
from src.collection.finance_collector import collect_finance
from src.collection.reddit_public_collector import collect_reddit_public


def main() -> None:
    cfg = load_config("config.yaml")

    # --- Finance ---
    fin = collect_finance(
        ticker=cfg["data"]["ticker"],
        years_back=int(cfg["data"]["start_years_back"]),
        interval=cfg["data"]["interval"],
        out_csv=cfg["paths"]["raw_prices_csv"],
        mongo_uri=cfg["mongo"]["uri"],
        mongo_db=cfg["mongo"]["db"],
        mongo_collection=cfg["mongo"]["collections"]["price_raw"],
    )

    print("\n=== Finance collection done ===")
    for k, v in fin.items():
        print(f"{k}: {v}")

    # --- Reddit (public JSON) ---
    red = collect_reddit_public(
        subreddits=cfg["reddit"]["subreddits"],
        keywords=cfg["reddit"]["keywords"],
        limit_per_subreddit=int(cfg["reddit"]["limit_per_subreddit"]),
        out_jsonl=cfg["paths"]["raw_reddit_jsonl"],
        mongo_uri=cfg["mongo"]["uri"],
        mongo_db=cfg["mongo"]["db"],
        mongo_collection=cfg["mongo"]["collections"]["social_raw"],
        user_agent="bigdata-tsla (academic) / 1.0",
        sleep_seconds=1.2,
    )

    print("\n=== Reddit collection done (public JSON) ===")
    for k, v in red.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
