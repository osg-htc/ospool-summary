#!/usr/bin/env bash

cd /home/clock/ospool-summary

source .venv/bin/activate

python3 -m cli summarize --env-file .env $(date +%Y-%m-%d)
