![Static Badge](https://img.shields.io/badge/Verified_Constants_On-2024%2F12%2F26-green?style=plastic)

# OSG Summary Script

Summarizes data daily for the OSPool to be used for reporting and visualizations.

## Usage

You need env variables set via a `.env` file or in your environment:

```json
ES_USER
ES_PASSWORD
ES_INDEX
ES_HOST
```

```shell
# On accounting3000 or with access to port 9200 on that host

# To summarize data for the day
python3 -m cli summarize --env-file .env 2025-03-01

# To validate previously generated data
python3 -m cli validate --env-file .env 2025-03-01

# To delete previously generated data
python3 -m cli delete --env-file .env 2025-03-01

# All of these commands can be run with a date range

# To summarize a week of data
python3 -m cli summarize --env-file .env 2025-03-01 2025-03-07
```

## Data Sources

- **Job Data** - Pulls data from OSG Adstash (osg-schedd-* index) on accounting3000
- **Resource Data** - Pulls data from Topology (https://github.com/opensciencegrid/topology/tree/master/topology)
- **User Data** - Pulls data from Topology (https://github.com/opensciencegrid/topology/tree/master/projects)
- **Institution Data** - Pulls data from Institution API (https://topology-institutions.osg-htc.org/ui/)

## Data Flow

1. Script runs on a K8 cron on Tiger
2. Data is pulled from accounting3000
3. Data is pushed to Elasticsearch on Tiger

## Development

```shell
ssh -L 9200:localhost:9200 -o ExitOnForwardFailure=yes accounting3000.chtc.wisc.edu
```
