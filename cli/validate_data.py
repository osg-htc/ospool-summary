
import os
from datetime import date, datetime, timedelta, timezone

import typer
from rich import print

from cli.util import get_current_date_count, get_date_summary_records
from summarize import get_summary_records
from summarize.es import index_documents
from summarize.validate import compare_summary_to_daily, daily_record_mapping, calculate_percent_difference


def validate_data(date: datetime, host: str, index: str, username: str, password: str, end: datetime = None):
    """Get yesterday's summary records and index them into Elasticsearch"""

    dates_to_validate = [date.date()]
    if end is not None:
        start_date = min(date.date(), end.date())
        end_date = max(date.date(), end.date())

        dates_to_validate = []
        i = start_date
        while i <= end_date:
            dates_to_validate.append(i)
            i += timedelta(days=1)

    comparisons = []
    for date in dates_to_validate:

        # Check existing summary documents state for the date
        summary_records = get_date_summary_records(date, host, index, username, password)
        comparison = compare_summary_to_daily(date, summary_records)
        max_diff = max([comparison[x] for x in comparison.keys() if "Vs" in x])

        pretty_dictionary = '\n'.join([f"{k}: {v}" for k, v in comparison.items()])

        # If we are off by > 5% then we should not push the data
        if max_diff > 5:
            print(f"[bold red]Data for {date} is off daily reports by {max_diff}%[/bold red]")
            print(f"[bold red]{pretty_dictionary}[/bold red]\n")
        else:
            print(f"[green]{pretty_dictionary}[/green]\n")

        if comparison["DailyJobs  "] != "?":
            comparisons.append(comparison)

    if len(comparisons) > 1:

        summary_comparison = {
            "Daily Jobs  ": sum([comparison["DailyJobs  "] for comparison in comparisons]),
            "Summary Jobs": sum([comparison["SummaryJobs"] for comparison in comparisons]),
            "Daily CPU Hours  ": sum([comparison["DailyCpuHours  "] for comparison in comparisons]),
            "Summary CPU Hours": sum([comparison["SummaryCpuHours"] for comparison in comparisons]),
            "Daily File Transfer Count  ": sum([comparison["DailyFileTransferCount  "] for comparison in comparisons]),
            "Summary File Transfer Count": sum([comparison["SummaryFileTransferCount"] for comparison in comparisons]),
            "Daily OSDF File Transfer Count  ": sum(
                [comparison["DailyOSDFFileTransferCount  "] for comparison in comparisons]),
            "Summary OSDF File Transfer Count": sum(
                [comparison["SummaryOSDFFileTransferCount"] for comparison in comparisons])
        }

        summary_comparison = {
            "Daily Vs Summary Jobs % Diff": calculate_percent_difference(summary_comparison["Daily Jobs  "],
                                                                         summary_comparison["Summary Jobs"]),
            "Daily Vs Summary CPU Hours % Diff": calculate_percent_difference(summary_comparison["Daily CPU Hours  "],
                                                                              summary_comparison["Summary CPU Hours"]),
            "Daily Vs Summary File Transfer Count % Diff": calculate_percent_difference(
                summary_comparison["Daily File Transfer Count  "], summary_comparison["Summary File Transfer Count"]),
            "Daily Vs Summary OSDF File Transfer Count % Diff": calculate_percent_difference(
                summary_comparison["Daily OSDF File Transfer Count  "],
                summary_comparison["Summary OSDF File Transfer Count"]),
            **summary_comparison
        }

        max_diff = max([summary_comparison[x] for x in summary_comparison.keys() if "Vs" in x])

        pretty_dictionary = '\n'.join([f"{k}: {v}" for k, v in summary_comparison.items()])

        # If we are off by > 5% then we should not push the data
        if max_diff > 5:
            print(f"[bold red]Data for {date} is off daily reports by {max_diff}%[/bold red]")
            print(f"[bold red]{pretty_dictionary}[/bold red]")
        else:
            print(f"[green]{pretty_dictionary}[/green]\n")


if __name__ == "__main__":
    """Used for debugging"""
    validate_data(
        datetime(2024, 1, 1),
        os.environ['ES_HOST'],
        os.environ['ES_INDEX'],
        os.environ['ES_USER'],
        os.environ['ES_PASSWORD'],
        datetime(2024, 1, 14),
    )
