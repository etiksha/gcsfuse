"""Extracts required metrics from fio output file and writes to google sheet.

   Takes fio output json filepath as command-line input
   Extracts IOPS, Bandwidth and Latency (min, max, mean) from given input file
   and writes the metrics in appropriate columns in a google sheet

   Usage: python3 fio_metrics.py <path to fio output json file>

"""

import json
import re
import sys
from typing import Any, Dict, List
sys.path.append('./gsheet')
import gsheet

JOBNAME = 'jobname'
GLOBAL_OPTS = 'global options'
JOBS = 'jobs'
JOB_OPTS = 'job options'
FILESIZE = 'filesize'
NUMJOBS = 'numjobs'
THREADS = 'num_threads'
TIMESTAMP_MS = 'timestamp_ms'
RUNTIME = 'runtime'
START_TIME = 'start_time'
END_TIME = 'end_time'
READ = 'read'
IOPS = 'iops'
BW = 'bw'
LAT = 'lat_ns'
MIN = 'min'
MAX = 'max'
MEAN = 'mean'

# Constants for writing to Google Sheets
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1kvHv1OBCzr9GnFxRu9RTJC7jjQjc9M4rAiDnhyak2Sg'
CREDENTIALS_PATH = ('/usr/local/google/home/ahanadatta/'
                    '.config/gspread/service_account.json')
WORKSHEET_NAME = 'fio_metrics!'
# cell containing the total number of entries in the sheet
# so that we know where the new entry has to be added
NUM_ENTRIES_CELL = 'N4'
UNITS = {BW: 'KiB/s', LAT: 'nsec', IOPS: ''}
FILESIZE_CONVERSION = {
    'b': 0.001,
    'k': 1,
    'kb': 1,
    'm': 10**3,
    'mb': 10**3,
    'g': 10**6,
    'gb': 10**6,
    't': 10**9,
    'tb': 10**9,
    'p': 10**12,
    'pb': 10**12
}


class NoValuesError(Exception):
  """Some data is missing from the json output file."""


class FioMetrics:
  """Handles logic related to parsing fio output and writing them to google sheet.

  """

  def _load_file_dict(self, filepath) -> Dict[str, Any]:
    """Reads json data from given filepath and returns json object.

    Args:
      filepath : str
        Path of the json file to be parsed

    Returns:
      JSON object, contains json data loaded from given filepath

    Raises:
      OSError: If input filepath doesn't exist
      ValueError: file is not in proper JSON format
      NoValuesError: file doesn't contain JSON data

    """
    fio_out = {}
    f = open(filepath, 'r')
    try:
      fio_out = json.load(f)
    except ValueError as e:
      raise e
    finally:
      f.close()

    if not fio_out:  # Empty JSON object
      raise NoValuesError(f'JSON file {filepath} returned empty object')
    return fio_out

  def _extract_metrics(self, fio_out) -> List[Dict[str, Any]]:
    """Extracts and returns required metrics from fio output dict.

      The extracted metrics are stored in a list. Each entry in the list is a
      dictionary. Each dictionary stores the following fio metrics related
      to a particualar job:
        jobname, filesize, number of threads, IOPS, Bandwidth and latency (min,
        max and mean)

    Args:
      fio_out: JSON object representing the fio output

    Returns:
      List of dicts, contains list of jobs and required metrics for each job
      Example return value:
        [{'jobname': '1_thread', 'filesize': 5000, 'num_threads':40,
        'start_time':1653027155, 'end_time':1653027215,
        'iops': 85.137657, 'bw': 99137, 'lat_ns': {'min': 365421594,
        'max': 38658496964, 'mean': 23292225875.57558}}]

    Raises:
      KeyError: Key is missing in the json output
      NoValuesError: Data not present in json object

    """

    if not fio_out:
      raise NoValuesError('No data in json object')

    global_filesize = ''
    if GLOBAL_OPTS in fio_out:
      if FILESIZE in fio_out[GLOBAL_OPTS]:
        global_filesize = fio_out[GLOBAL_OPTS][FILESIZE]

    all_jobs = []
    prev_endtime = 0
    for i, job in enumerate(fio_out[JOBS]):
      jobname = ''
      iops = bw_kibps = min_lat_ns = max_lat_ns = mean_lat_ns = filesize = 0
      jobname = job[JOBNAME]
      # for multiple jobs, start time of one job = end time of previous job
      start_time = prev_endtime * 1000 if prev_endtime > 0 else fio_out[
          TIMESTAMP_MS]
      job_read = job[READ]
      end_time = start_time + job_read[RUNTIME]
      start_time = start_time // 1000
      end_time = round(end_time/1000)
      prev_endtime = end_time
      iops = job_read[IOPS]
      bw_kibps = job_read[BW]
      min_lat_ns = job_read[LAT][MIN]
      max_lat_ns = job_read[LAT][MAX]
      mean_lat_ns = job_read[LAT][MEAN]
      numjobs = '1'
      if JOB_OPTS in job:
        if NUMJOBS in job[JOB_OPTS]:
          numjobs = job[JOB_OPTS][NUMJOBS]

        if FILESIZE in job[JOB_OPTS]:
          filesize = job[JOB_OPTS][FILESIZE]

      if not filesize:
        filesize = global_filesize

      # If jobname and filesize are empty OR start_time=end_time
      # OR all the metrics are zero, log skip warning and continue to next job
      if ((not jobname and not filesize) or (start_time == end_time) or
          (not iops and not bw_kibps and not min_lat_ns and not max_lat_ns and
           not mean_lat_ns)):
        # TODO(ahanadatta): Print statement will be replaced by google logging.
        print(f'No job details or metrics in json, skipping job index {i}')
        prev_endtime = 0
        continue

      filesize_num, filesize_unit = re.findall('[0-9]+|[A-Za-z]+', filesize)
      mult_factor = FILESIZE_CONVERSION[filesize_unit.lower()]
      filesize_kb = int(filesize_num) * mult_factor

      numjobs = int(numjobs)

      all_jobs.append({
          JOBNAME: jobname,
          FILESIZE: filesize_kb,
          THREADS: numjobs,
          START_TIME: start_time,
          END_TIME: end_time,
          IOPS: iops,
          BW: bw_kibps,
          LAT: {MIN: min_lat_ns, MAX: max_lat_ns, MEAN: mean_lat_ns}
      })

    if not all_jobs:
      raise NoValuesError('No data could be extracted from file')

    return all_jobs

  def _add_to_gsheet(self, jobs):
    """Add the metric values to respective columns in a google sheet.

    Args:
      jobs: list of dicts, contains required metrics for each job
    """

    values = []
    for job in jobs:
      values.append((job[JOBNAME], job[FILESIZE], job[THREADS], job[START_TIME],
                     job[END_TIME], job[IOPS], job[BW], job[LAT][MIN],
                     job[LAT][MAX], job[LAT][MEAN]))
    gsheet.write_to_google_sheet('fio_metrics!', values)

  def get_metrics(self, filepath, add_to_gsheets=True) -> List[Dict[str, Any]]:
    """Returns job metrics obtained from given filepath and writes to gsheets.

    Args:
      filepath : str
        Path of the json file to be parsed
      add_to_gsheets: bool, optional, default:True
        Whether job metrics should be written to Google sheets or not

    Returns:
      List of dicts, contains list of jobs and required metrics for each job
    """
    fio_out = self._load_file_dict(filepath)
    job_metrics = self._extract_metrics(fio_out)
    if add_to_gsheets:
      self._add_to_gsheet(job_metrics)

    return job_metrics

if __name__ == '__main__':
  argv = sys.argv
  if len(argv) != 2:
    raise TypeError('Incorrect number of arguments.\n'
                    'Usage: '
                    'python3 fio_metrics.py <fio output json filepath>')

  fio_metrics_obj = FioMetrics()
  temp = fio_metrics_obj.get_metrics(argv[1], True)
  print(temp)
