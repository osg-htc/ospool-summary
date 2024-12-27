
import os
from datetime import date, datetime, timedelta
from numbers import Number

import typer
from rich import print
import pandas as pd

from cli.util import get_current_date_count
from summarize.es import search, delete_by_query


def report(host, index):
    """Print out a pretty report on the quality of the data for a given date"""

    query = get_query()
    response = search(query, host, index)

    df = es_response_to_df(response)

    df.to_csv("summary_20241113.csv", index=False)


def es_response_to_df(es_response):
    """Convert ES response to a pandas dataframe"""

    buckets = es_response['aggregations']['ProjectName']['buckets']

    df_prep = {}
    for bucket in buckets:
        project_name = bucket['key']
        df_prep[project_name] = {}
        df_prep[project_name]['ProjectName'] = bucket['key']

        for key in bucket.keys():
            if key in {'key', 'doc_count'}:
                continue

            df_prep[project_name][key] = bucket[key]['value']

    df = pd.DataFrame.from_dict(df_prep, orient='index')

    return df


def get_query():
    """Get 'Project' or 'Resource' query"""

    return {
        "query": {
            "range": {
                "Date": {
                    "gte": datetime(year=2024, month=11, day=13).isoformat(),
                    "lt": (datetime(year=2024, month=11, day=13) + timedelta(seconds=1)).isoformat()
                }
            }
        },
        "aggs": {
            "ProjectName": {
                "terms": {
                    "field": "ProjectName.keyword",
                    "size": 10000
                },
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
                    }
                }
            }
        }
    }


if __name__ == "__main__":
    """Used for debugging"""
    report(
        os.environ['ES_HOST'],
        os.environ['ES_INDEX']
    )
