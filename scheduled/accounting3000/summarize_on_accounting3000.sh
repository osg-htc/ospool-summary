#!/usr/bin/env bash

cd /home/clock/ospool-summary

source .venv/bin/activate

python3 -m cli summarize --dry-run --env-file .env $(date -d "yesterday" +%Y-%m-%d)
