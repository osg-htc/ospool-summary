import datetime

import requests
import pandas as pd
import numpy as np

comparison = []

daily_record_mapping = {
    "num_uniq_job_ids": "NumJobs",
    "all_cpu_hours": "CpuHours",
    "total_files_xferd": 'FileTransferCount',
    "osdf_files_xferd": 'OSDFFileTransferCount'
}


def compare_summary_to_daily(date: datetime.date, summary_records: list):
    """Compares the summary records we generated to the canonical daily reports"""

    summary_agg_keys = daily_record_mapping.values()
    summary_aggregates = {
        k: sum([record[k] for record in summary_records if record[k] is not None]) for k in summary_agg_keys
    }

    query = {
        "query": {
            "terms": {
                "_id": [f"OSG-schedd-job-history_daily_{date}"]
            }
        }
    }

    daily_report = requests.get("http://localhost:9200/daily_totals/_search", json=query)
    daily_report_json = daily_report.json()
    daily_report_list = daily_report_json["hits"]["hits"]

    # If there are no records published that day, return 100% difference
    if len(daily_report_list) == 0:
        print(f"[yellow]Could not find daily report for {date}[/yellow]")
        return {
            "Date": date,

            "DailyVsSummaryJobs": 100,
            "DailyVsSummaryCpuHours": 100,
            "DailyVsSummaryFileTransferCount": 100,
            "DailyVsSummaryOSDFFileTransferCount": 100,

            "DailyJobs  ": "?",
            "SummaryJobs": summary_aggregates["NumJobs"],
            "DailyCpuHours  ": "?",
            "SummaryCpuHours": summary_aggregates["CpuHours"],
            "DailyFileTransferCount  ": "?",
            "SummaryFileTransferCount": summary_aggregates["FileTransferCount"],
            "DailyOSDFFileTransferCount  ": "?",
            "SummaryOSDFFileTransferCount": summary_aggregates["OSDFFileTransferCount"]
        }
    daily_report = daily_report_list[0]["_source"]

    # Compare the two dictionaries base on the daily_record_mapping add to csv
    return {
        "Date": date,

        **calculate_differences(daily_report, summary_aggregates),

        "DailyJobs  ": daily_report["num_uniq_job_ids"],
        "SummaryJobs": summary_aggregates["NumJobs"],
        "DailyCpuHours  ": daily_report["all_cpu_hours"],
        "SummaryCpuHours": summary_aggregates["CpuHours"],
        "DailyFileTransferCount  ": daily_report["total_files_xferd"],
        "SummaryFileTransferCount": summary_aggregates["FileTransferCount"],
        "DailyOSDFFileTransferCount  ": daily_report["osdf_files_xferd"],
        "SummaryOSDFFileTransferCount": summary_aggregates["OSDFFileTransferCount"]
    }


def calculate_differences(daily_report: dict, summary_aggregates: dict):

    return {
        "DailyVsSummaryJobs": calculate_percent_difference(daily_report["num_uniq_job_ids"],
                                                           summary_aggregates["NumJobs"]),
        "DailyVsSummaryCpuHours": calculate_percent_difference(daily_report["all_cpu_hours"],
                                                               summary_aggregates["CpuHours"]),
        "DailyVsSummaryFileTransferCount": calculate_percent_difference(daily_report["total_files_xferd"],
                                                                        summary_aggregates["FileTransferCount"]),
        "DailyVsSummaryOSDFFileTransferCount": calculate_percent_difference(daily_report["osdf_files_xferd"],
                                                                            summary_aggregates[
                                                                                "OSDFFileTransferCount"]),
    }


def calculate_percent_difference(x: float, y: float) -> float:
    """Calculates the percent difference between the daily and summary reports"""

    if x == 0 and y == 0:
        return 0.0

    return (abs(x - y) / ((x + y) / 2)) * 100


if __name__ == "__main__":
    pass
