#!/usr/bin/env python3
import sys

for line in sys.stdin:
    # Skip empty lines
    line = line.strip()
    if not line:
        continue

    # Just pass the line to reducer
    print(line)
