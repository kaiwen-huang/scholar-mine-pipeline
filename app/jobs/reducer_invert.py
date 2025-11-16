#!/usr/bin/env python3
import sys
from collections import defaultdict

current_word = None
doc_list = []

for line in sys.stdin:
    word, doc_id = line.strip().split("\t", 1)
    if current_word and word != current_word:
        print(f"{current_word}\t{','.join(doc_list)}")
        doc_list = []
    current_word = word
    doc_list.append(doc_id)

if current_word:
    print(f"{current_word}\t{','.join(doc_list)}")
