![Static Badge](https://img.shields.io/badge/Verified_Constants_On-2024%2F12%2F26-green?style=plastic)

# OSG Summary Script

Generates the authoritative summary data for the OSPool to be used for OSG reporting and visualizations.

## Data Sources

- **Job Data** - Pulls data from OSG Adstash (osg-schedd-* index) on accounting3000
- **Resource Data** - Pulls data from Topology (https://github.com/opensciencegrid/topology/tree/master/topology)
- **User Data** - Pulls data from Topology (https://github.com/opensciencegrid/topology/tree/master/projects)
- **Institution Data** - Pulls data from Institution API (https://topology-institutions.osg-htc.org/ui/)

## Data Flow

1. Script runs on a K8 cron on Tiger
2. Data is pulled from accounting3000
3. Data is pushed to Elasticsearch on Tiger

# Oddities

Record of things that need context or explanation.

# Things

# What is a OSPool Job?

- A job that is submitted to the OSG

## Development

```shell
ssh -L 9200:localhost:9200 -o ExitOnForwardFailure=yes accounting3000.chtc.wisc.edu
```
