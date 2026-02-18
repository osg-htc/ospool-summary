import os
import sys
from io import StringIO
from datetime import datetime, timedelta
from pathlib import Path
import logging
from typing import Optional
from typing_extensions import Annotated

import dotenv
import typer

from cli.delete_date import delete_date as delete_date_cli
from cli.push_summary_date import push_summary_date
from cli.report_quality import report_quality as report_quality_cli
from cli.validate_data import validate_data as validate_data_cli
from util.send_email import send_email

app = typer.Typer()


@app.command()
def delete(date: datetime, end: Annotated[Optional[datetime], typer.Argument()] = None, env_file: str = None, debug: bool = False, force: bool = False):
    """
    Deletes all documents for a given date or range
    """

    # Setup
    setup_logging(debug)
    load_env_file(env_file, "ES_USER", "ES_PASSWORD", "ES_HOST", "ES_INDEX")

    delete_date_cli(date, os.environ['ES_HOST'], os.environ['ES_INDEX'], os.environ['ES_USER'], os.environ['ES_PASSWORD'], force, end=end)


@app.command()
def summarize(date: datetime, end: Annotated[Optional[datetime], typer.Argument()] = None, env_file: str = None, debug: bool = False, force: bool = False, dry_run: bool = False, not_interactive: bool = False, regenerate: bool = False, send_failure_email: bool = False):
    """
    Summarizes and pushes the OSPool summary data for a given date

    :param date: The date to summarize
    :param env_file: The path to the environment file
    :param debug: Whether to enable debug logging
    :param force: Whether to force the push of the summary data if the data is off by more than 5%
    """

    # Setup
    setup_logging(debug)
    load_env_file(env_file, "ES_USER", "ES_PASSWORD", "ES_HOST", "ES_INDEX", "ES_PROVIDER_HOST")

    email_body = f"""
    Push summary for {date} to {end if end else date}.

    These summaries are the source of information of the OSPool webpages. If they fail the data will not be updated,
    and the dates will be stuck on the last successful summary push.

    These summaries are completed by the image found at git@github.com:osg-htc/ospool-summary.git/images/summarize_yesterday:latest.

    Everyday we summarize the previous day's data, every weekend we resummarize last years data in case mapped values have changed.
    """

    # Capture stdout from the push_summary_date function
    captured_output = StringIO()
    old_stdout = sys.stdout
    sys.stdout = captured_output

    try:
        push_summary_date(date, os.environ['ES_PROVIDER_HOST'], os.environ['ES_HOST'],  os.environ['ES_INDEX'], os.environ['ES_USER'], os.environ['ES_PASSWORD'], force, dry_run, not_interactive, regenerate, end)

        # Restore stdout and get captured output
        sys.stdout = old_stdout
        output_text = captured_output.getvalue()

        send_email(
            'chtc-cron-mailto@chtc.io',
            'chtc-cron-mailto@g-groups.wisc.edu',
            "✅✅✅✅✅ - OSPool Summary Push Succeeded",
            email_body + f"\n\nOutput:\n{output_text}",
        )
    except Exception as e:
        # Restore stdout
        sys.stdout = old_stdout
        output_text = captured_output.getvalue()

        # If we are sending an email on failure
        if send_failure_email:

            # Feels dumb to hardcode but so does spending time to consider
            send_email(
                'chtc-cron-mailto@chtc.io',
                'chtc-cron-mailto@g-groups.wisc.edu',
                "🔥🔥🔥🔥🔥 - OSPool Summary Push Failure",
                email_body + f"\n\nOutput:\n{output_text}\n\nError details:\n{str(e)}",
            )

        raise e
    finally:
        # Ensure stdout is always restored
        sys.stdout = old_stdout
        captured_output.close()


@app.command()
def validate(date: datetime, end: Annotated[Optional[datetime], typer.Argument()] = None, env_file: str = None, debug: bool = False):
    """
    Validates the OSPool summary data for a given date based on daily report benchmarks

    :param date: The date to validate
    :param env_file: The path to the environment file
    :param debug: Whether to enable debug logging
    :param end: The end date for validation
    """

    # Setup
    setup_logging(debug)
    load_env_file(env_file, "ES_USER", "ES_PASSWORD", "ES_HOST", "ES_INDEX", "ES_PROVIDER_HOST")

    # Validate the data
    validate_data_cli(date, os.environ['ES_PROVIDER_HOST'], os.environ['ES_HOST'], os.environ['ES_INDEX'], os.environ['ES_USER'], os.environ['ES_PASSWORD'], end=end)


@app.command()
def report_quality(env_file: str = None, debug: bool = False):
    """
    Reports out the number of resources that are left unmapped due to missing resource and project mappings
    """

    # Setup
    setup_logging(debug)
    load_env_file(env_file, "ES_HOST", "ES_INDEX")

    report_quality_cli(os.environ['ES_HOST'], os.environ['ES_INDEX'])


def setup_logging(debug: bool = False):
    """Set the logging level"""

    # Set the logging level
    if debug:
        logging.basicConfig(level=logging.DEBUG)


def load_env_file(env_file: str, *required_env_vars: str):
    """Load the environment file and check for the required environment variables"""

    # Load the environment file if provided
    if env_file is not None:
        if Path(env_file).exists():
            dotenv.load_dotenv(dotenv_path=env_file)
        else:
            typer.echo(f"File {env_file} does not exist")
            raise typer.Exit(code=1)

    # Check for required environment variables
    missing_env_var = False
    for var in required_env_vars:
        if var not in os.environ:
            typer.echo(f"Required environment variable {var} not set")
            missing_env_var = True
    if missing_env_var:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
