#!/usr/bin/env bash

python3 -m cli summarize --send-failure-email $(date -d "yesterday" +%Y-%m-%d)
