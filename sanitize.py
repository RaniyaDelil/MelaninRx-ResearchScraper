# sanitize.py
import re, orjson, os, sys
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE = re.compile(r"\b(?:\+?\d{1,3}[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?)?\d{3}[-.\s]?\d{4}\b")
def scrub(text):
    if not text:
        return text
    text = EMAIL.sub("[email]", text)
    text = PHONE.sub("[phone]", text)
    return text

def run(infile="data_raw/reddit_posts.jsonl", outfile="data_public/sample_sanitized.jsonl", maxrows=50):
    if not os.path.exists("data_public"):
        os.makedirs("data_public")
    i=0
    with open(infile,"rb") as inf, open(outfile,"wb") as outf:
        for line in inf:
            if i >= maxrows: break
            rec = orjson.loads(line)
            rec["text"] = scrub(rec.get("text",""))
            rec["title"] = scrub(rec.get("title",""))
            # Keep author hashed only (already hashed)
            rec.pop("url", None)  # remove permalink to be extra safe
            outf.write(orjson.dumps(rec)+b"\n")
            i+=1
    print("Wrote sample:", outfile)

if __name__=="__main__":
    run()
