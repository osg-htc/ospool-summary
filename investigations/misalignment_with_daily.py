import datetime

import requests
import pandas as pd
import numpy as np

from summarize.main import get_summary_records

comparison = []

daily_record_mapping = {
    "num_uniq_job_ids": "NumJobs",
    "all_cpu_hours": "CpuHours",
    "total_files_xferd": 'FileTransferCount',
    "osdf_files_xferd": 'OSDFFileTransferCount'
}

yesterday = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=datetime.timezone(datetime.timedelta(hours=-6))) - datetime.timedelta(days=1)

date = yesterday
for x in range(30):

    summary_records = get_summary_records(date)

    summary_agg_keys = daily_record_mapping.values()
    summary_aggregates = {
        k: sum([record[k] for record in summary_records if record[k] is not None]) for k in summary_agg_keys
    }

    daily_report = requests.get(f"https://raw.githubusercontent.com/osg-htc/ospool-data/refs/heads/master/data/daily_reports/{date.date()}.json").json()

    # Compare the two dictionaries base on the daily_record_mapping add to csv
    comparison.append({
        "Date": date.date(),
        "DailyVsSummaryJobs": daily_report["num_uniq_job_ids"] / (summary_aggregates["NumJobs"] if summary_aggregates["NumJobs"] > 0 else np.nan),
        "DailyVsSummaryCpuHours": daily_report["all_cpu_hours"] / (summary_aggregates["CpuHours"] if summary_aggregates["NumJobs"] > 0 else np.nan),
        "DailyVsSummaryFileTransferCount": daily_report["total_files_xferd"] / (summary_aggregates["FileTransferCount"] if summary_aggregates["NumJobs"] > 0 else np.nan),
        "DailyVsSummaryOSDFFileTransferCount": daily_report["osdf_files_xferd"] / (summary_aggregates["OSDFFileTransferCount"] if summary_aggregates["NumJobs"] > 0 else np.nan),
        "DailyJobs": daily_report["num_uniq_job_ids"],
        "SummaryJobs": summary_aggregates["NumJobs"],
        "DailyCpuHours": daily_report["all_cpu_hours"],
        "SummaryCpuHours": summary_aggregates["CpuHours"],
        "DailyFileTransferCount": daily_report["total_files_xferd"],
        "SummaryFileTransferCount": summary_aggregates["FileTransferCount"],
        "DailyOSDFFileTransferCount": daily_report["osdf_files_xferd"],
        "SummaryOSDFFileTransferCount": summary_aggregates["OSDFFileTransferCount"]
    })

    date -= datetime.timedelta(days=1)

df = pd.DataFrame(comparison)
df.to_csv('daily_vs_summary_all.csv', index=False)

# Print out the summary columns
x = df.describe()
