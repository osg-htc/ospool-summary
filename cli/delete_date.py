
import os
from datetime import date, datetime, timedelta

import typer
from rich import print

from cli.util import get_current_date_count
from summarize.es import search, delete_by_query


def delete_date(date: datetime, host, index, username: str, password: str, force: bool = False, end: datetime = None):
    """Delete all documents for a given date or range"""

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

        date_document_count = get_current_date_count(date, host, index, username, password)

        confirmed = force or typer.confirm(
            f"Confirm deletion of {date_document_count} documents from {index} on date {date}?"
        )

        if not confirmed:
            raise typer.Exit()

        date = datetime.combine(date, datetime.min.time())

        query = {
            "query": {
                "range": {
                    "Date": {
                        "gte": date.isoformat(),
                        "lt": (date + timedelta(seconds=1)).isoformat()
                    }
                }
            }
        }

        try:
            delete_by_query(query, host, index, username, password)
        except Exception as e:
            print(f"[bold red]Failed to delete documents: {e}[/bold red]")
            raise typer.Exit(code=1)
        else:
            print(f"[green]Deleted {date_document_count} documents from {date}![/green]")


if __name__ == "__main__":
    """Used for debugging"""
    delete_date(
        date(2025, 1, 6),
        os.environ['ES_HOST'],
        os.environ['ES_INDEX'],
        os.environ['ES_USER'],
        os.environ['ES_PASSWORD']
    )
