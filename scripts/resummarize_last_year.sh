#!/usr/bin/env bash

python3 -m cli summarize --send-failure-email --regenerate --not-interactive $(date -d "1 year ago" +%Y-%m-%d) $(date -d "yesterday" +%Y-%m-%d)
