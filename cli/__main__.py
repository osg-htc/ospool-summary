import os
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
def summarize(date: datetime, end: Annotated[Optional[datetime], typer.Argument()] = None, env_file: str = None, debug: bool = False, force: bool = False, dry_run: bool = False):
    """
    Summarizes and pushes the OSPool summary data for a given date

    :param date: The date to summarize
    :param env_file: The path to the environment file
    :param debug: Whether to enable debug logging
    :param force: Whether to force the push of the summary data if the data is off by more than 5%
    """

    # Setup
    setup_logging(debug)
    load_env_file(env_file, "ES_USER", "ES_PASSWORD", "ES_HOST", "ES_INDEX")

    push_summary_date(date, os.environ['ES_HOST'], os.environ['ES_INDEX'], os.environ['ES_USER'], os.environ['ES_PASSWORD'], force, dry_run, end)


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
    load_env_file(env_file, "ES_USER", "ES_PASSWORD", "ES_HOST", "ES_INDEX")

    # Validate the data
    validate_data_cli(date, os.environ['ES_HOST'], os.environ['ES_INDEX'], os.environ['ES_USER'], os.environ['ES_PASSWORD'], end=end)


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


def load_env_file(env_file: str | None, *required_env_vars: str):
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
