# reddit_ingest.py
import os, time, hashlib, orjson
from datetime import datetime, timezone
from tenacity import retry, wait_exponential, stop_after_attempt
from dotenv import load_dotenv
import praw
from tqdm import tqdm

load_dotenv()
CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
USER_AGENT = os.getenv("REDDIT_USER_AGENT")

SUBREDDITS = [
    "blackladies", "BlackWomen", "BabyBumps", "pregnant", "AskWomen"
]
KEYWORDS = [
    "pregnancy", "postpartum", "prenatal", "obgyn", "ob-gyn", "midwife", "doula",
    "birth trauma", "c-section", "cesarean", "VBAC", "racism", "discrimination", "doctor"
]
QUERY = " OR ".join(f'"{k}"' if " " in k else k for k in KEYWORDS)

OUT = "data_raw/reddit_posts.jsonl"
MAX_PER_SUB = 30   # small to keep total ~50-100

def ahash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def as_iso(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

@retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(5))
def query_subreddit(r, sub_name):
    return r.subreddit(sub_name).search(QUERY, time_filter="all", limit=MAX_PER_SUB, sort="relevance")

def main():
    if not os.path.exists("data_raw"):
        os.makedirs("data_raw")
    reddit = praw.Reddit(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, user_agent=USER_AGENT, check_for_async=False)
    seen = set()
    with open(OUT, "ab") as f:
        for sub in SUBREDDITS:
            print("Searching r/" + sub)
            try:
                results = list(query_subreddit(reddit, sub))
            except Exception as e:
                print("Error querying", sub, e)
                continue
            for p in results:
                if p.id in seen: 
                    continue
                seen.add(p.id)
                rec = {
                    "platform": "reddit",
                    "community": f"r/{sub}",
                    "post_id": p.id,
                    "author_hash": ahash(p.author.name) if p.author else None,
                    "created_utc": as_iso(p.created_utc),
                    "title": (p.title or ""),
                    "text": (p.selftext or ""),
                    "url": f"https://www.reddit.com{p.permalink}",
                    "score": getattr(p, "score", None),
                    "num_comments": getattr(p, "num_comments", None)
                }
                f.write(orjson.dumps(rec) + b"\n")
                time.sleep(0.2)
    print("Wrote:", OUT)

if __name__ == "__main__":
    main()
