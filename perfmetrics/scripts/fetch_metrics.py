"""Executes fio_metrics.py and vm_metrics.py by passing appropriate arguments.
"""
import socket
import sys
from fio import fio_metrics
import time
from datetime import datetime
import asyncio
from vm_metrics import vm_metrics

START_TIME = 'start_time'
END_TIME = 'end_time'
INSTANCE = socket.gethostname()
PERIOD = 120

if __name__ == '__main__':
#   asyncio.run(fun())
  argv = sys.argv
  if len(argv) != 2:
    raise TypeError('Incorrect number of arguments.\n'
                    'Usage: '
                    'python3 fetch_metrics.py <fio output json filepath>')

  fio_metrics_obj = fio_metrics.FioMetrics()
  print("fio metrics start:")
  print(datetime.fromtimestamp(time.time()))
  temp = fio_metrics_obj.get_metrics(argv[1], True)
  print("fio metrics end and sleep start:")
  print(datetime.fromtimestamp(time.time()))
  time.sleep(250)
  print(datetime.fromtimestamp(time.time()))
  vm_metrics_obj = vm_metrics.VmMetrics()
  for job in temp:
    start_time_sec = job[START_TIME]
    end_time_sec = job[END_TIME]
    print("job")
    print(datetime.fromtimestamp(time.time()))
    print(datetime.fromtimestamp(start_time_sec))
    print(datetime.fromtimestamp(end_time_sec))
    vm_metrics_obj.fetch_metrics_and_write_to_google_sheet(start_time_sec,
                                                     end_time_sec, INSTANCE,
                                                     PERIOD)
 
