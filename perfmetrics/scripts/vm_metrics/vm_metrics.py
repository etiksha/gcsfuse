"""
To execute the script:
>>python3 vm_metrics.py {instance} {start time in epoch sec} {end time in epoch sec} {mean period in sec}
The code takes input the start time and end time (in epoch seconds) and the
instance name and the mean period. Then it creates an instance of VmMetrics
class and calls its methods with all the corresponding parameters, makes the api
call, reads the response and returns a list of metric points where each point
has the peak value and mean value of a mean period. Then it dumps all the data
into a google sheet.
"""
import sys
import os
import time
import datetime
import dataclasses
import google.api_core
import google.cloud
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import monitoring_v3
from gsheet import gsheet

WORKSHEET_NAME = 'vm_metrics!'

PROJECT_NAME = 'projects/gcs-fuse-test'
CPU_UTI_METRIC = 'compute.googleapis.com/instance/cpu/utilization'
RECEIVED_BYTES_COUNT_METRIC = 'compute.googleapis.com/instance/network/received_bytes_count'
OPS_ERROR_COUNT_METRIC = 'custom.googleapis.com/gcsfuse/fs/ops_error_count'
OPS_LATENCY_METRIC = 'custom.googleapis.com/gcsfuse/fs/ops_latency'
READ_BYTES_COUNT_METRIC = 'custom.googleapis.com/gcsfuse/gcs/read_bytes_count'


class NoValuesError(Exception):
  """API response values are missing."""


@dataclasses.dataclass
class MetricPoint:
  value: float
  start_time_sec: int
  end_time_sec: int

  
def _parse_metric_value_by_type(value, value_type) -> float:
  if value_type == 3:
    return value.double_value
  elif value_type == 2:
    return value.int64_value
  elif value_type == 5:
    return value.distribution_value.mean
  else:
    raise Exception('Unhandled Value type')


def _create_metric_points_from_response(metrics_response, factor):
  """Parses the given peak and mean metrics and returns a list of MetricPoint.
    Args:
      peak_metrics_response (object): The peak metrics API response
      mean_metrics_response (object): The mean metrics API response
      factor (float) : For converting the API response values into appropriate units.
    Returns:
      list[MetricPoint]
    Raises:
      NoValuesError: Raise when API response is empty.
  """
  metric_point_list = []
  for metric in metrics_response:
    for point in metric.points:
      value = _parse_metric_value_by_type(point.value, metric.value_type)
      metric_point = MetricPoint(value / factor,
                                 point.interval.start_time.seconds,
                                 point.interval.end_time.seconds)

      metric_point_list.append(metric_point)

  if len(metric_point_list) == 0:
    raise NoValuesError('No values were retrieved from the call')
  metric_point_list.reverse()
  return metric_point_list


class VmMetrics:

  def _validate_start_end_times(self, start_time_sec, end_time_sec):
    """Checks that start time is less than end time.
    Args:
      start_time_sec (int) : Epoch seconds
      end_time_sec (int) : Epoch seconds
    Raises:
      ValueError : When start time is after end time.
    """
    if start_time_sec < end_time_sec:
      return True
    else:
      raise ValueError('Start time should be before end time')

  def _get_api_response(self, metric_type, start_time_sec, end_time_sec,
                        instance, period, aligner, fs_op):
    """Fetches the API response for peak and mean metrics.
    Args:
      metric_type (str): The type of metric
      start_time_sec (int): Epoch seconds
      end_time_sec (int): Epoch seconds
      instance (str): VM instance name
      period (float): Period over which the mean and peak values are taken
    Returns:
      list[peak metrics API response, mean metrics API response]
    Raises:
      GoogleAPICallError
    """

    client = monitoring_v3.MetricServiceClient()
    interval = monitoring_v3.TimeInterval(
        end_time={'seconds': int(end_time_sec)},
        start_time={'seconds': int(start_time_sec)})

    aggregation = monitoring_v3.Aggregation(
        alignment_period={'seconds': period},
        per_series_aligner=getattr(monitoring_v3.Aggregation.Aligner
        ,aligner)
    )

    if(metric_type[0:7]=='compute'):
        metric_filter = ('metric.type = "{metric_type}" AND '
                     'metric.label.instance_name ={instance_name}').format(
                         metric_type=metric_type, instance_name=instance)
    elif(metric_type[0:6]=='custom'):
        if(fs_op == ''):
            metric_filter = ('metric.type = "{metric_type}" AND '
                     'metric.labels.opencensus_task = ends_with("{instance_name}") ').format(
                      metric_type=metric_type, instance_name=instance)
        else:
            metric_filter = ('metric.type = "{metric_type}" AND '
                    'metric.labels.opencensus_task = ends_with("{instance_name}") AND '
                    'metric.labels.fs_op = {fs_op}').format(
                    metric_type=metric_type, instance_name=instance, fs_op=fs_op)

    try:
      metrics_response = client.list_time_series({
          'name': PROJECT_NAME,
          'filter': metric_filter,
          'interval': interval,
          'view': monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
          'aggregation': aggregation,
      })
    except:
      raise GoogleAPICallError('The request for peak response of ' +
                               metric_type + ' failed, Please try again.')
    return metrics_response

  def _get_metrics(self, start_time_sec, end_time_sec, instance, period,
                   metric_type, factor, aligner, fs_op):
    """Returns the metrics data for requested metric type.
    Args:
      start_time_sec (int): Epoch seconds
      end_time_sec (int): Epoch seconds
      instance (str): VM instance
      period (float): Period over which the mean and peak values are taken
      metric_type (str): The metric whose data is to be retrieved
      factor (float) : The factor by which the value of API response should be
                       divided to get inot desired units.
    Returns:
      list[MetricPoint]
    """
    metrics_response = self._get_api_response(
        metric_type, start_time_sec, end_time_sec, instance, period, aligner, fs_op)
    metrics_data = _create_metric_points_from_response(metrics_response, factor)
    return metrics_data

  def fetch_metrics_and_write_to_google_sheet(self, start_time_sec,
                                              end_time_sec, instance,
                                              period) -> None:
    """Fetches the metrics data for cpu utilization and received bytes count and writes it to a google sheet.
    Args:
      start_time_sec (int): Epoch seconds
      end_time_sec (int): Epoch seconds
      instance (str): VM instance
      period (float): Period over which the mean and peak values are taken
    Returns: None
    """
    self._validate_start_end_times(start_time_sec, end_time_sec)
    cpu_uti_peak_data = self._get_metrics(start_time_sec, end_time_sec, instance,
                                     period, CPU_UTI_METRIC, 1 / 100, 'ALIGN_MAX', '')
    cpu_uti_mean_data = self._get_metrics(start_time_sec, end_time_sec, instance,
                                     period, CPU_UTI_METRIC, 1 / 100, 'ALIGN_MEAN', '')
    rec_bytes_peak_data = self._get_metrics(start_time_sec, end_time_sec, instance,
                                       period, RECEIVED_BYTES_COUNT_METRIC,
                                       60, 'ALIGN_MAX', '')
    rec_bytes_mean_data = self._get_metrics(start_time_sec, end_time_sec, instance,
                                       period, RECEIVED_BYTES_COUNT_METRIC,
                                       60, 'ALIGN_MEAN', '')
    ops_error_count_data = self._get_metrics(start_time_sec, end_time_sec, instance,
                                       period, OPS_ERROR_COUNT_METRIC,
                                       1, 'ALIGN_DELTA', 'GetXattr')
    ops_latency_mean_data = self._get_metrics(start_time_sec, end_time_sec, instance,
                                       period, OPS_LATENCY_METRIC,
                                       1, 'ALIGN_DELTA', 'ReadFile')
    read_bytes_count_data = self._get_metrics(start_time_sec, end_time_sec, instance,
                                       period, READ_BYTES_COUNT_METRIC,
                                       1, 'ALIGN_DELTA', '')      
                                                              
    metrics_data = []
    for cpu_uti_peak, cpu_uti_mean, rec_bytes_peak, rec_bytes_mean, ops_error_count, ops_latency, read_bytes_count in zip(
        cpu_uti_peak_data, cpu_uti_mean_data, rec_bytes_peak_data, rec_bytes_mean_data, ops_error_count_data, ops_latency_mean_data, read_bytes_count_data):
      metrics_data.append([
          cpu_uti_peak.start_time_sec, cpu_uti_peak.value, cpu_uti_mean.value,
          rec_bytes_peak.value, rec_bytes_mean.value,
          ops_error_count.value, ops_latency.value, read_bytes_count.value
      ])

    # Writing metrics data to google sheet
    gsheet.write_to_google_sheet(WORKSHEET_NAME, metrics_data)


def main() -> None:
  if len(sys.argv) != 5:
    raise Exception('Invalid arguments.')
  instance = sys.argv[1]
  start_time_sec = int(sys.argv[2])
  end_time_sec = int(sys.argv[3])
  period = int(sys.argv[4])
  vm_metrics = VmMetrics()
  vm_metrics.fetch_metrics_and_write_to_google_sheet(start_time_sec,
                                                     end_time_sec, instance,
                                                     period)

if __name__ == '__main__':
  main()
