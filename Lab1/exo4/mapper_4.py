import sys
from datetime import datetime

for line in sys.stdin:
    fields = line.strip().split(',')

    if "regains" in fields[3].lower():
        event_date = fields[1] 
        location = fields[0]    

        date_obj = datetime.strptime(event_date, "%m/%d/%y")
        formatted_date = date_obj.strftime("%Y-%m-%d")

        print("{}\t{}".format(location, formatted_date))
