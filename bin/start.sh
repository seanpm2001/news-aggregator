#!/bin/bash

set -euo pipefail
IFS=$'\n\t'

task="${1-run-all}"

mkdir -p output

function die_usage() {
  echo "$0 usage:"
  echo "  defaults to 'csv-to-json' command"
  echo ""
  echo "  $0 csv-to-json                 Read to single source CSV and convert it into JSON"
  echo "  $0 global-json                 Read all the sources CSV and convert it into GLOBAL JSON"
  echo "  $0 feed-processor              Start a feed-processor-multi"
  echo "  $0 run-all                     Run all the required end-to-end (For deployment)"
  echo "  $0 shell                       Start a bpython shell"
  exit 1
}

if [[ "$task" = "csv-to-json" ]]; then
  if [[ ! -d "output/" ]]; then
    echo "Error: output/ dir not found!"
    echo "Are you in the from root directory?"
    exit 1
  fi
  set -x
  echo "Init feed sources"
  python -u src/csv_to_json.py feed.json



elif [[ "$task" = "global-json" ]]; then
  if [[ ! -d "output/" ]]; then
    echo "Error: output/ dir not found!"
    echo "Are you in the from root directory?"
    exit 1
  fi
  set -x
  echo "Generating sources.global.json"
  python -u src/csv_to_global_json.py

elif [[ "$task" = "feed-processor" ]]; then
  set -x
  echo "Starting main script..."
  mkdir -p output/feed/cache
  python -u src/feed_processor_multi.py

elif [[ "$task" = "run-all" ]]; then
  if [[ ! -d "output/" ]]; then
    echo "Error: output/ dir not found!"
    echo "Are you in the from root directory?"
    exit 1

  set -x

  echo "Waiting for squid to start..."
  seconds_remaining=60
  while [ $seconds_remaining -gt 0  ] && [ ! pids=$(pidof squid) ]
  do
     sleep 1
     echo $seconds_remaining
     seconds_remaining="$(($seconds_remaining - 1))"
  done
  if [ ! pids=$(pidof squid) ]
  then
    exit
  fi

  echo "Init feed sources"
  python -u src/csv_to_json.py feed.json

  echo "Generating sources.global.json"
  python -u src/csv_to_global_json.py

  echo "Starting main script..."
  mkdir -p output/feed/cache
  python -u src/feed_processor_multi.py

  echo "Shutting down squid..."
  kill $(pidof squid)

elif [[ "$task" = "shell" ]]; then
  set -x
  bpython

elif [[ "$task" = "help" || "$task" = "-h" || "$task" = "--help" ]]; then
  die_usage

else
  echo "unknown cmd: $task"
  die_usage
fi
