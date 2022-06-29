
#!/bin/bash
set -e
echo Running fio test..
fio job_files/job_7.fio --lat_percentiles 1 --output-format=json --output='output.json' --experimental-stackdriver-export-interval=60s
echo Installing requirements..
pip install -r requirements.txt --user
echo Adding pytest to PATH:
export PATH=/home/kbuilder/.local/bin:$PATH
gsutil cp gs://gcs-fuse-dashboard-fio/creds.json ./gsheet
echo Running tests..
#pytest gsheet/gsheet_test.py
#pytest vm_metrics/vm_metrics_test.py
echo Fetching results..
python3 fetch_metrics.py output.json
