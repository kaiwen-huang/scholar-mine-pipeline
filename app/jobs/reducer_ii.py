# Aggregate counts for each term
import sys
current = None
count = 0

for line in sys.stdin:
    term, val = line.strip().split("\t")
    if current == term:
        count += int(val)
    else:
        if current:
            print(f"{current}\t{count}")
        current = term
        count = int(val)

if current:
    print(f"{current}\t{count}")
