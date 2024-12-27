
import os
from datetime import date, datetime, timedelta
from numbers import Number

import typer
from rich import print

from cli.util import get_current_date_count
from summarize.es import search, delete_by_query


def report_quality(host, index):
    """Print out a pretty report on the quality of the data for a given date"""

    total_query = get_query(None)
    total_response = search(total_query, host, index)

    project_query = get_query("Project")

    project_response = search(project_query, host, index)

    if project_response['hits']['total']['value'] == 0:
        print("[bold green]All Projects Mapped[/bold green]")

    else:
        print(f"[bold red]{print_unmapped_resource_information(project_response, total_response, 'ProjectNames')}[/bold red]")

    resource_query = get_query("Resource")
    resource_response = search(resource_query, host, index)

    if resource_response['hits']['total']['value'] == 0:
        print("[bold green]All Resources Mapped[/bold green]")

    else:
        print(f"[bold red]{print_unmapped_resource_information(resource_response, total_response, 'ResourceNames')}[/bold red]")


def print_unmapped_resource_information(term_response, total_response, term_key: str):
    agg_keys = [
        "NumJobs",
        "CpuHours",
        "GpuHours",
        "FileTransferCount",
        "ByteTransferCount",
        "OSDFFileTransferCount",
        "OSDFByteTransferCount"
    ]

    max_key_length = max([len(k) for k in agg_keys])

    term_key_values = [x['key'] for x in term_response['aggregations'][term_key]['buckets']]

    if len(term_key_values) == 0:
        return ""

    s = f"Unmapped {term_key}: {term_key_values}\n\nResources Left Unmapped\n"

    for agg_key in agg_keys:
        ljust_key = (agg_key + ":").ljust(max_key_length + 1)

        term_value = round(term_response['aggregations'][agg_key]['value'], 2)
        total_value = round(total_response['aggregations'][agg_key]['value'], 2)
        percent_of_total = round((term_value / total_value) * 100, 2)

        s += f"{ljust_key} {term_value}/{percent_of_total}% of Total\n"

    return s


def get_query(term: str, start=None, end=None):
    """Get 'Project' or 'Resource' query"""


    query = {}
    if term:
        query = {
            "query": {
                "bool": {
                    "must_not": [
                        {
                            "exists": {
                                "field": f"{term}Institution.id"
                            }
                        }
                    ]
                }
            }
        }

    return {
        "track_total_hits": True,
        "size": 0,
        **query,
        "aggs": {
            "NumJobs": {
                "sum": {
                    "field": "NumJobs"
                }
            },
            "FileTransferCount": {
                "sum": {
                    "field": "FileTransferCount"
                }
            },
            "ByteTransferCount": {
                "sum": {
                    "field": "ByteTransferCount"
                }
            },
            "CpuHours": {
                "sum": {
                    "field": "CpuHours"
                }
            },
            "GpuHours": {
                "sum": {
                    "field": "GpuHours"
                }
            },
            "OSDFFileTransferCount": {
                "sum": {
                    "field": "OSDFFileTransferCount"
                }
            },
            "OSDFByteTransferCount": {
                "sum": {
                    "field": "OSDFByteTransferCount"
                }
            },
            "ProjectNames": {
                "terms": {
                    "field": "ProjectName.keyword",
                    "size": 10000
                }
            },
            "ResourceNames": {
                "terms": {
                    "field": "ResourceName.keyword",
                    "size": 10000
                }
            }
        }
    }


if __name__ == "__main__":
    """Used for debugging"""
    report_quality(
        os.environ['ES_HOST'],
        os.environ['ES_INDEX']
    )
