# reddit_ingest.py
import os, time, hashlib, orjson, re
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

PREG_TERMS = [
    "pregnancy","postpartum","prenatal","obgyn","ob-gyn","midwife","doula",
    "birth trauma","c-section","cesarean","vbac","contractions",
    "gestational","trimester","morning sickness","breastfeeding","lactation","pelvic floor",
    "epidural","home birth","stillbirth","miscarriage","preeclampsia","gbs","postnatal",
    "pregnant","ttc","fertility","ivf"
]

# reserved for future filtering/analysis
CONTEXT_TERMS = ["racism","discrimination","doctor","hospital","insurance","bias"]

# NOTE: keeping plain "black" can introduce noise; consider preferring phrases.
BLACK_TERMS = ["black","african-american","bipoc","african",
               "black woman","black women","black maternal","black mothers"]

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s.lower().replace("–","-").replace("—","-")).strip()

# single-word terms get word-boundary regex; phrases stay as substrings
_wordlike = [t for t in PREG_TERMS if " " not in t and "-" not in t]
PREG_WORD_RE = re.compile(r"\b(" + "|".join(map(re.escape, _wordlike)) + r")\b")
# allow dashed/space variants for obgyn forms
OBGYN_RE = re.compile(r"\b(ob[\s-]?gyn|ob[\s-]?g[\s-]?yn|ob-?g-?yn)\b")
# phrases we’ll check with `in`
PREG_PHRASES = [t for t in PREG_TERMS if " " in t or "-" in t]

def _preg_matches(t: str):
    hits = set(PREG_WORD_RE.findall(t))
    if OBGYN_RE.search(t):
        hits.add("obgyn")
    for ph in PREG_PHRASES:
        if ph in t:
            hits.add(ph)
    return sorted(hits)

def _black_matches(t: str):
    return sorted([b for b in BLACK_TERMS if b in t])

# On Black-focused subs, only pregnancy terms needed; on general subs require pregnancy AND Black terms
def _q_or(terms): return " OR ".join(f'"{k}"' if " " in k else k for k in terms)
QUERY_PREG_ONLY = _q_or(PREG_TERMS)
QUERY_PREG_AND_BLACK = f'({QUERY_PREG_ONLY}) AND ({_q_or(BLACK_TERMS)})'

OUT = "data_raw/reddit_posts.jsonl"
MAX_PER_SUB = 200  # tune to hit ~50–100 total

def ahash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def as_iso(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

def is_relevant(text: str, community: str):
    t = _norm(text)
    preg_hits = _preg_matches(t)
    if not preg_hits:
        return False
    # On general subs, also require Black context in text
    if community.lower() not in ["r/blackwomen", "r/blackladies"]:
        return bool(_black_matches(t))
    return True  # Black-focused subs: pregnancy term alone is enough

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
            q = build_query(sub)  # use the right query per subreddit
            try:
                results = list(search_subreddit(reddit, sub, q))
            except Exception as e:
                print("Error querying", sub, e)
                continue

            for p in results:
                if p.id in seen:
                    continue
                seen.add(p.id)
                title = (getattr(p, "title", "") or "")
                body = (getattr(p, "selftext", "") or "")
                community = f"r/{sub}"

                text_all = f"{title} {body}"
                if not is_relevant(text_all, community):
                    continue

                tnorm = _norm(text_all)
                likely_black = True if community.lower() in ["r/blackwomen", "r/blackladies"] else bool(_black_matches(tnorm))

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
                    "likely_black_context": likely_black,
                    "matched_preg_terms": _preg_matches(tnorm),
                    "matched_black_terms": _black_matches(tnorm)
                }
                f.write(orjson.dumps(rec) + b"\n")
                saved += 1
                time.sleep(0.2)
    print(f"Wrote: {OUT}  (saved {saved} records)")

if __name__ == "__main__":
    main()
