
#!/bin/bash
set -e
echo Running fio test..
date
fio job_files/job_5.fio --output-format=json --output='output.json'
date
cat output.json
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
