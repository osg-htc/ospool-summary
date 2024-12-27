from datetime import datetime, timedelta
import os

from summarize.es import search


def get_date_summary_records(date: datetime.date, host: str, index: str, username: str, password: str):
    """Get all records for a given date"""

    date = datetime.combine(date, datetime.min.time())

    query = {
        "size": 10000,
        "query": {
            "range": {
                "Date": {
                    "gte": date.isoformat(),
                    "lt": (date + timedelta(seconds=1)).isoformat()
                }
            }
        }
    }

    data = search(query, host, index, username, password)

    return [x['_source'] for x in data['hits']['hits']]


def get_current_date_count(date: datetime.date, host: str, index: str, username: str, password: str):
    """Get current # of documents ingested on a date"""

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

    count_query = {
        "track_total_hits": True,
        "size": 0,
        **query
    }

    data = search(count_query, host, index, username, password)

    return data['hits']['total']['value']


if __name__ == "__main__":
    get_date_records(
        datetime(2024, 11, 13).date(),
        os.environ['ES_HOST'],
        os.environ['ES_INDEX'],
        os.environ['ES_USER'],
        os.environ['ES_PASSWORD']
    )
