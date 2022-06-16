"""Executes fio_metrics.py and vmmetrics.py by passing appropriate arguments.
"""
import socket
import sys
from fio import fio_metrics
from vmmetrics import vmmetrics

START_TIME = 'start_time'
END_TIME = 'end_time'
INSTANCE = socket.gethostname()
PERIOD = 120

if __name__ == '__main__':
  argv = sys.argv
  if len(argv) != 2:
    raise TypeError('Incorrect number of arguments.\n'
                    'Usage: '
                    'python3 execute_codes.py <fio output json filepath>')

  fio_metrics_obj = fio_metrics.FioMetrics()
  temp = fio_metrics_obj.get_metrics(argv[1], True)
  vm_metrics_obj = vmmetrics.VmMetrics()
  for job in temp:
    start_time_sec = job[START_TIME]
    end_time_sec = job[END_TIME]
    vm_metrics_obj.fetch_metrics_and_write_to_google_sheet(start_time_sec,
                                                     end_time_sec, INSTANCE,
                                                     PERIOD)
 
