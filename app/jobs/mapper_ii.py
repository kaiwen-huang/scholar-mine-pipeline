# Extract words from abstract text and emit (word, 1)
import sys, json, re

# Simple tokenizer + stopword removal
stopwords = set(open("stopwords.txt").read().split())

for line in sys.stdin:
    try:
        record = json.loads(line.strip())
        abstract = record.get("abstract", "")
    except Exception:
        continue
    for token in re.findall(r"[a-z]+", abstract.lower()):
        if token not in stopwords:
            print(f"{token}\t1")
