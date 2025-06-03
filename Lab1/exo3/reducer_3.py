#!/usr/bin/env python3
import sys

current_store = None
total_sales = 0

for line in sys.stdin:
    store, cost = line.strip().split('\t')
    cost = float(cost)
    
    if store == current_store:
        total_sales += cost
    else:
        if current_store:
            # Print previous store's total
            print("{}\t{:.2f}".format(current_store, total_sales))
        current_store = store
        total_sales = cost

# Print the last store
if current_store:
    print("{}\t{:.2f}".format(current_store, total_sales))