import requests


def get_summaries_with_missing_data():
    """
    Get a list of reports that are missing data
    """

    response = requests.get(
        "https://raw.githubusercontent.com/osg-htc/ospool-data/refs/heads/master/data/daily_reports/missing_data.json"
    )

    return response.json()