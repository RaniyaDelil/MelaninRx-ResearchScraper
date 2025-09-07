# reddit_ingest.py
import os, time, hashlib, orjson
from datetime import datetime, timezone
from tenacity import retry, wait_exponential, stop_after_attempt
from dotenv import load_dotenv
import praw

load_dotenv()
CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
USER_AGENT = os.getenv("REDDIT_USER_AGENT")

SUBREDDITS = [
    "blackladies", "BlackWomen", "BabyBumps", "pregnant", "AskWomen"
]

KEYWORDS = [
    "pregnancy", "postpartum", "prenatal", "obgyn", "ob-gyn", "midwife", "doula",
    "birth trauma", "c-section", "cesarean", "vbac", "racism", "discrimination", "doctor"
]
BLACK_TERMS = ["black","african-american","bipoc","african"]

# On Black-focused subs, only pregnancy terms needed; on general subs require pregnancy AND Black terms
QUERY_PREG_ONLY = " OR ".join(f'"{k}"' if " " in k else k for k in KEYWORDS)
QUERY_PREG_AND_BLACK = f'({QUERY_PREG_ONLY}) AND (' + " OR ".join(BLACK_TERMS) + ")"

OUT = "data_raw/reddit_posts.jsonl"
MAX_PER_SUB = 30   # tune to hit ~50–100 total

def ahash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def as_iso(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

def is_relevant(text, community):
    t = text.lower()
    if community.lower() in ["r/blackwomen", "r/blackladies"]:
        return any(k in t for k in KEYWORDS)  # only pregnancy terms needed
    return (any(k in t for k in KEYWORDS) and any(b in t for b in BLACK_TERMS))

def build_query(sub_name):
    return QUERY_PREG_ONLY if sub_name.lower() in ["blackwomen","blackladies"] else QUERY_PREG_AND_BLACK

@retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(5))
def search_subreddit(r, sub_name, query):
    # time_filter: 'day','week','month','year','all' — use 'all' for breadth
    return r.subreddit(sub_name).search(query, time_filter="all", limit=MAX_PER_SUB, sort="relevance")

def main():
    os.makedirs("data_raw", exist_ok=True)
    reddit = praw.Reddit(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        user_agent=USER_AGENT,
        check_for_async=False
    )
    seen = set()
    saved = 0
    with open(OUT, "ab") as f:
        for sub in SUBREDDITS:
            print("Searching r/" + sub)
            q = build_query(sub)                      # <-- use the right query per subreddit
            try:
                results = list(search_subreddit(reddit, sub, q))
            except Exception as e:
                print("Error querying", sub, e)
                continue

            for p in results:
                if p.id in seen:
                    continue
                seen.add(p.id)
                title = (p.title or "")
                body = (p.selftext or "")
                community = f"r/{sub}"
                relevant = is_relevant(title + " " + body, community)
                if not relevant:
                    continue

                rec = {
                    "platform": "reddit",
                    "community": community,
                    "post_id": p.id,
                    "author_hash": ahash(p.author.name) if p.author else None,
                    "created_utc": as_iso(p.created_utc),
                    "title": title,
                    "text": body,
                    "url": f"https://www.reddit.com{p.permalink}",
                    "score": getattr(p, "score", None),
                    "num_comments": getattr(p, "num_comments", None),
                    "likely_black_context": True
                }
                f.write(orjson.dumps(rec) + b"\n")
                saved += 1
                time.sleep(0.2)
    print(f"Wrote: {OUT}  (saved {saved} records)")

if __name__ == "__main__":
    main()
