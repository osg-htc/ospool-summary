
import pytz
import os
from datetime import date, datetime, timedelta, timezone

import typer
from rich import print

from cli.util import get_current_date_count
from summarize import get_summary_records
from summarize.es import index_documents
from summarize.validate import compare_summary_to_daily


def push_summary_date(date: datetime, host: str, index: str, username: str, password: str, force: bool = False, dry_run: bool = False, not_interactive: bool = False, end: datetime = None):
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

    for date in dates_to_validate:

        # Check existing summary documents state for the date
        date_document_count = get_current_date_count(date, host, index, username, password)
        if date_document_count > 0 and not dry_run:
            print(f"[bold red]Documents already exist for {date}, please delete before updating[/bold red]")
            raise typer.Exit(code=1)

        # Get the start of the day in Central Time
        start_time = datetime.combine(date, datetime.min.time())
        end_time = start_time + timedelta(days=1)

        # To keep things consistent with the reports convert to central time
        start_central_time = start_time.astimezone(pytz.timezone('America/Chicago'))
        end_central_time = end_time.astimezone(pytz.timezone('America/Chicago'))

        # Get the summary records
        summary_records = get_summary_records(start=start_central_time, end=end_central_time)

        comparison = compare_summary_to_daily(date, summary_records)
        max_diff = max([comparison[x] for x in comparison.keys() if "Vs" in x])

        pretty_dictionary = '\n'.join([f"{k}: {v}" for k, v in comparison.items()])

        # If we are off by > 5% then we should not push the data
        if max_diff > 5:
            print(f"[bold red]Data for {date} is off daily reports by {max_diff}%[/bold red]")
            print(f"[bold red]{pretty_dictionary}[/bold red]")

            # If not forcing via cli, ask for confirmation if you want to force
            if not force and not dry_run:

                # If interactive and user opts in
                if not not_interactive and typer.confirm("Index these documents despite warnings?", default=False):
                    print(f"[yellow]Force indexing {len(summary_records)} documents on {date}[/yellow]")

                else:
                    raise typer.Exit(code=1)
        else:
            print(f"[green]{pretty_dictionary}[/green]\n")

        # Index the summary records
        if not dry_run:
            try:
                index_documents(summary_records, host, index, username, password)
            except Exception as e:
                print(f"[bold red]Failed to index documents[/bold red]")
                raise e
            else:
                print(f"[green]Indexed {len(summary_records)} documents![/green]")


if __name__ == "__main__":
    """Used for debugging"""
    push_summary_date(
        date(2024, 1, 1),
        os.environ['ES_HOST'],
        os.environ['ES_INDEX'],
        os.environ['ES_USER'],
        os.environ['ES_PASSWORD']
   )
