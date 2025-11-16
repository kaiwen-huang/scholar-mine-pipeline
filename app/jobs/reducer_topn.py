#!/usr/bin/env python3
import sys
import os
import heapq

TOP_N = int(os.getenv("TOP_N", "20"))

records = []  # Each item is (count, term)

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    # Support both "term\tcount" and "term count"
    if "\t" in line:
        parts = line.split("\t")
    else:
        parts = line.split()

    if len(parts) != 2:
        continue

    term, cnt = parts[0].strip(), parts[1].strip()
    try:
        c = int(cnt)
    except ValueError:
        continue

    records.append((c, term))

# Take top N by count descending
top = heapq.nlargest(TOP_N, records, key=lambda x: x[0])

for c, term in top:
    # Output format: "term\tcount"
    print(f"{term}\t{c}")
