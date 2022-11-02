import time
from datetime import datetime, timedelta

start_date = datetime(2021, 7, 15, 10, 0, 0)
print(start_date)
ref_date = datetime.now()
time.sleep(5)
now_date = datetime.now()
delta = now_date - ref_date
incr = timedelta(minutes=59, seconds=55)
print(delta + incr)
start_date += incr + delta
print(start_date)


