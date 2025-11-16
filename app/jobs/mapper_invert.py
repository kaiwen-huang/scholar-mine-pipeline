#!/usr/bin/env python3
import sys, json, re

for line in sys.stdin:
    record = json.loads(line)
    doc_id = record.get("doc_id")
    text = record.get("abstract", "")
    words = re.findall(r"[a-zA-Z]+", text.lower())
    for w in words:
        print(f"{w}\t{doc_id}")
