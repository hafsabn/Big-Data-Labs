import sys
from collections import defaultdict

location_dates = defaultdict(list)

for line in sys.stdin:
    fields = line.strip().split('\t')  
    location = fields[0]
    date = fields[1]
    location_dates[location].append(date)

for location, dates in location_dates.items():
    sorted_dates = sorted(dates)
    print("{}\t{}".format(location, ','.join(sorted_dates)))
