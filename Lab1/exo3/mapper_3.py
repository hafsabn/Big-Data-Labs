#!/usr/bin/env python3
import sys

for line in sys.stdin:
    # Split line into fields: date, time, store, product, cost, payment
    fields = line.strip().split('\t')
    
    # Ensure we have exactly 6 fields
    if len(fields) != 6:
        continue
    
    try:
        store = fields[2]
        cost = float(fields[4])
        print("{}\t{}".format(store, cost))
    except ValueError:
        # Skip lines with invalid cost format
        continue