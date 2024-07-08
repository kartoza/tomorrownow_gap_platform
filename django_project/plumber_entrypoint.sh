#!/bin/sh

# Exit script in case of error
set -e

echo $"\n\n\n"
echo "-----------------------------------------------------"
echo "STARTING PLUMBER ENTRYPOINT $(date)"
echo "-----------------------------------------------------"

# Run initialization
cd /home/web/django_project
echo 'Running plumber_initialize.py...'
python3 -u plumber_initialize.py

echo "-----------------------------------------------------"
echo "FINISHED PLUMBER ENTRYPOINT --------------------------"
echo "-----------------------------------------------------"

# start worker for Plumber
celery -A core worker -c 1 -Q plumber -l INFO -n plumberworker
