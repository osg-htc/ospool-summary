"""
Accounting3000 interaction module

Provides summaries from the adstash index. This is a read-only module.
"""
import pickle
from pathlib import Path
import datetime
import requests
import json
import logging
import copy
from functools import lru_cache
from collections import defaultdict
import os

# Configure logging
logger = logging.getLogger(__name__)


def get_ospool_ad_summary(start: datetime.datetime, end: datetime.datetime, host: str = "http://localhost:9200"):

    logger.debug(f"Querying from {start.timestamp()} to {end.timestamp()}")

    query = {
        "track_total_hits": True,
        "size": 0,
        "aggs": {
            "institution_id": {
                "terms": {
                    "field": "MachineAttrOSG_INSTITUTION_ID0.keyword",
                    "missing": "UNKNOWN",
                    "size": 1024
                },
                "aggs": {
                    "resources": {
                        "terms": {
                            "field": "ResourceName",
                            "missing": "UNKNOWN",
                            "size": 1024
                        },
                        "aggs": {
                            "acct_group": {
                                "terms": {
                                    "field": "ProjectName.keyword",
                                    "missing": "UNKNOWN",
                                    "size": 1024
                                },
                                "aggs": {
                                    "gpu_hours": {
                                        "sum": {
                                            "field": "GpuCoreHr"
                                        }
                                    },
                                    "cpu_hours": {
                                        "sum": {
                                            "field": "CoreHr"
                                        }
                                    },
                                    **get_transfer_aggregates(host)
                                },
                            },
                        }
                    }
                },
            }
        },
        "runtime_mappings": {
            "ResourceName": {
                "type": "keyword",
                "script": {
                    "language": "painless",
                    "source": """
                String res;
                if (doc.containsKey("MachineAttrGLIDEIN_ResourceName0") && doc["MachineAttrGLIDEIN_ResourceName0.keyword"].size() > 0) {
                    res = doc["MachineAttrGLIDEIN_ResourceName0.keyword"].value;
                } else if (doc.containsKey("MATCH_EXP_JOBGLIDEIN_ResourceName") && doc["MATCH_EXP_JOBGLIDEIN_ResourceName.keyword"].size() > 0) {
                    res = doc["MATCH_EXP_JOBGLIDEIN_ResourceName.keyword"].value;
                } else {
                    res = "UNKNOWN";
                }
                emit(res);
                """,
                }
            }
        },
        "query": {
            "bool": {
                "filter": [
                    {
                        "range": {
                            "RecordTime": {
                                "gte": int(start.timestamp()),
                                "lt": int(end.timestamp())
                            }
                        }
                    }
                ],
                "minimum_should_match": 1,
                "should": [
                    {
                        "bool": {
                            "filter": [
                                {
                                    "terms": {
                                        # The job must have been submitted to one of the OSPool Access Points
                                        "ScheddName.keyword": list(get_ospool_aps())
                                    }
                                },
                            ],
                            "must_not": [
                                {
                                    "exists": {
                                        "field": "LastRemotePool",
                                    }
                                },
                            ],
                        }
                    },
                    {
                        "terms": {
                            # Resource must have one of the OSPool Collectors as the LastRemotePool
                            "LastRemotePool.keyword": list(OSPOOL_COLLECTOR_HOSTS)
                        }
                    },
                ],
                "must_not": [
                    {
                        "terms": {
                            "JobUniverse": JOB_UNIVERSES_TO_SKIP
                        }
                    },
                    # Currently disabled so that this matches with Jason's reports from JobAccounting repo
                    # {
                    #     "terms": {
                    #         # Resource must not be in the non-fairshare list
                    #         "ResourceName": list(OSPOOL_NON_FAIRSHARE_RESOURCES)
                    #     }
                    # },
                ],
            }
        }
    }

    logger.debug(json.dumps(query, sort_keys=True, indent=2))

    # Pull out the document and dump it in a dated file
    response = requests.get(
        f"{host}/osg-schedd-*/_search",
        data=json.dumps(query, sort_keys=True, indent=2),
        headers={'Content-Type': 'application/json'},
        verify=False
    )
    response_json = response.json()

    logger.debug(f"Got {response_json['hits']['total']['value']} records")
    logger.debug(get_document_bin_counts([*map(lambda x: x['_source'], response_json['hits']['hits'])]))

    check_response_failure(response_json)

    flat_response = flatten_aggregates(response_json, host)

    logger.debug(f"Got {len(flat_response)} records")
    logger.debug(f"Summary Statistic: {print_flat_response(flat_response)}")

    return flat_response


def get_document_bin_counts(docs: dict):
    """Dump all the dict values into a set and return the counts - Useful when numbers don't align"""

    bin_counts = defaultdict(dict)
    for doc in docs:
        for key, value in doc.items():

            # If the value is a int, str or bool then toss it in a bin
            if isinstance(value, (int, str, bool)):
                bin_counts[key][value] = bin_counts[key].get(value, 0) + 1

    # Run through and remove all the keys that have a count of 1
    for k0 in bin_counts.keys():
        for k1 in [*bin_counts[k0].keys()]:
            if bin_counts[k0][k1] == 1:
                del bin_counts[k0][k1]

    return json.dumps(bin_counts, indent=2)


def get_schedd_collector_host_map(update=False):
    """Get the Schedd to CollectorHost mapping for the OSPool"""

    if update:
        update_schedd_collector_host_map()

    schedd_collector_host_map_pickle = Path("./data/ospool-host-map.pkl")
    schedd_collector_host_map = {}
    if schedd_collector_host_map_pickle.exists():
        try:
            schedd_collector_host_map = pickle.load(open(schedd_collector_host_map_pickle, "rb"))
        except IOError:
            pass
    else:
        logger.debug("No pickle found, just using custom mappings")

    schedd_collector_host_map.update(CUSTOM_MAPPING)

    return schedd_collector_host_map


def update_schedd_collector_host_map():
    """Update the Schedd to CollectorHost mapping for the OSPool - **Not needed if the file is symlinked in**"""

    import htcondor2

    original_schedd_collector_host_map = get_schedd_collector_host_map()
    schedd_collector_host_map = copy.deepcopy(original_schedd_collector_host_map)

    collector = htcondor2.Collector(OSPOOL_COLLECTOR)

    schedds = [ad["Machine"] for ad in collector.locateAll(htcondor2.DaemonTypes.Schedd)]

    for schedd in schedds:
        schedd_collector_host_map[schedd] = set()

        for collector_host in OSPOOL_COLLECTOR_HOSTS:
            collector = htcondor2.Collector(collector_host)
            ads = collector.query(
                htcondor2.AdTypes.Schedd,
                constraint=f'''Machine == "{schedd.split('@')[-1]}"''',
                projection=["Machine", "CollectorHost"],
            )
            ads = list(ads)
            if len(ads) == 0:
                continue
            if len(ads) > 1:
                logger.debug(f'Got multiple Schedd ClassAds for Machine == "{schedd}"')

            # Cache the CollectorHost in the map
            if "CollectorHost" in ads[0]:
                schedd_collector_hosts = set()
                for schedd_collector_host in ads[0]["CollectorHost"].split(","):
                    schedd_collector_host = schedd_collector_host.strip().split(":")[0]
                    if schedd_collector_host:
                        schedd_collector_hosts.add(schedd_collector_host)
                if schedd_collector_hosts:
                    schedd_collector_host_map[schedd] = schedd_collector_hosts
                    break
        else:
            logger.debug(f"Did not find Machine == {schedd} in collectors")

    # Update the pickle
    with open("./data/ospool-host-map.pkl", "wb") as f:

        # Report the number of new mappings added
        logger.debug("Added {len(schedd_collector_host_map) - len(original_schedd_collector_host_map)} new Schedd to CollectorHost mappings")

        pickle.dump(schedd_collector_host_map, f)


def get_ospool_aps():
    """Get the list of OSPool Access Points"""

    aps = set()
    ap_collector_host_map = get_schedd_collector_host_map()
    for ap, collectors in ap_collector_host_map.items():
        if ap.startswith("jupyter-notebook-") or ap.startswith("jupyterlab-"):
            continue
        if len(collectors & OSPOOL_COLLECTOR_HOSTS) > 0:
            aps.add(ap)
    return aps


def get_transfer_aggregates(host):
    """Create the json for transfer aggregates"""

    keys = get_transfer_keys_for_bytes_and_files(host)

    agg_query = {}
    for key in keys:
        agg_query[key] = {
            "sum": {
                "field": key
            }
        }

    return agg_query


@lru_cache(maxsize=1)
def get_transfer_keys_for_bytes_and_files(host):
    """Get the all file transfer keys for aggregation"""

    response = requests.get(
        f"{host}/osg-schedd-*/_mapping?pretty",
        headers={
            'Content-Type': 'application/json'
        },
        verify=False
    )

    keys = set()
    for index, mapping in response.json().items():
        if "properties" in mapping["mappings"]:
            index_properties = mapping["mappings"]["properties"]

            # Add in the input stat keys
            if "TransferInputStats" in index_properties and "properties" in index_properties["TransferInputStats"]:
                keys.update(f"TransferInputStats.{key}" for key in index_properties['TransferInputStats']['properties'].keys())

            # Add in the output stat keys
            if "TransferOutputStats" in index_properties and "properties" in index_properties["TransferOutputStats"]:
                keys.update(f"TransferOutputStats.{key}" for key in index_properties['TransferOutputStats']['properties'].keys())

    # Filter out the ones that are not bytes or files
    keys = {key for key in keys if ("FilesCountTotal".casefold() in key.casefold() or "SizeBytesTotal".casefold() in key.casefold())}

    return keys

# List of job universes to not count
JOB_UNIVERSES_TO_SKIP = [
    7,  # Scheduler Universe
    12  # Local Universe
]

# List of job statuses to not count
JOB_STATUSES_TO_SKIP = [
    3,  # Removed
]

# List of OSPool Collectors
OSPOOL_COLLECTOR = "cm-1.ospool.osg-htc.org"
OSPOOL_COLLECTOR_HOSTS = {
    "cm-1.ospool.osg-htc.org",
    "cm-2.ospool.osg-htc.org",
    "flock.opensciencegrid.org",
}

# List of non-fairshare resources
OSPOOL_NON_FAIRSHARE_RESOURCES = {
    "SURFsara",
    "NIKHEF-ELPROD",
    "INFN-T1",
    "IN2P3-CC",
    "UIUC-ICC-SPT",
    "TACC-Frontera-CE2"
}

# Additional set of mappings for custom access points
CUSTOM_MAPPING = {
    "osg-login2.pace.gatech.edu": {"osg-login2.pace.gatech.edu"},
    "ce1.opensciencegrid.org": {"cm-2.ospool.osg-htc.org", "cm-1.ospool.osg-htc.org"},
    "login-test.osgconnect.net": {"cm-2.ospool.osg-htc.org", "cm-1.ospool.osg-htc.org"},
    "scosg16.jlab.org": {"scicollector.jlab.org", "osg-jlab-1.t2.ucsd.edu"},
    "scosgdev16.jlab.org": {"scicollector.jlab.org", "osg-jlab-1.t2.ucsd.edu"},
    "submit6.chtc.wisc.edu": {"htcondor-cm-path.osg.chtc.io"},
    "login-el7.xenon.ci-connect.net": {"cm-2.ospool.osg-htc.org", "cm-1.ospool.osg-htc.org"},
    "login.collab.ci-connect.net": {"cm-2.ospool.osg-htc.org", "cm-1.ospool.osg-htc.org"},
    "uclhc-2.ps.uci.edu": {"uclhc-2.ps.uci.edu"},
    "osgsub01.sdcc.bnl.gov": {"scicollector.jlab.org", "osg-jlab-1.t2.ucsd.edu"},
}


def flatten_aggregates(aggregates, host):
    """Flatten the nested aggregates"""

    transfer_keys = get_transfer_keys_for_bytes_and_files(host)
    file_keys = [key for key in transfer_keys if "FilesCountTotal".casefold() in key.casefold()]
    byte_keys = [key for key in transfer_keys if "SizeBytesTotal".casefold() in key.casefold()]

    # Calculate OSDF transfers
    osdf_file_keys = [key for key in file_keys if "osdf" in key.casefold() or "stash" in key.casefold()]
    osdf_byte_keys = [key for key in byte_keys if "osdf" in key.casefold() or "stash" in key.casefold()]

    resources = []
    for institution_id in aggregates['aggregations']["institution_id"]["buckets"]:
        for resource in institution_id["resources"]["buckets"]:
            for acct_group in resource["acct_group"]["buckets"]:

                resources.append({
                    "isNRP": institution_id["key"],
                    "InstitutionID": institution_id["key"],
                    "ResourceName": resource["key"],
                    "AcctGroup": acct_group["key"],
                    "NumJobs": acct_group["doc_count"],
                    "GpuHours": acct_group["gpu_hours"]["value"],
                    "CpuHours": acct_group["cpu_hours"]["value"],
                    "OSDFFileTransferCount": sum([acct_group[key]["value"] if key in acct_group else 0 for key in osdf_file_keys]),
                    "OSDFByteTransferCount": sum([acct_group[key]["value"] if key in acct_group else 0 for key in osdf_byte_keys]),
                    "FileTransferCount": sum([acct_group[key]["value"] if key in acct_group else 0 for key in file_keys]),
                    "ByteTransferCount": sum([acct_group[key]["value"] if key in acct_group else 0 for key in byte_keys]),
                    **{k: acct_group[k]["value"] if k in acct_group else 0 for k in transfer_keys},
                })

    return resources


def print_flat_response(flat_response):
    """Print the flat response"""

    s = ""
    for key in flat_response[0].keys():
        if isinstance(flat_response[0][key], (int, float)):
            s += f"{key.ljust(max(map(lambda x: len(str(x)), flat_response[0].keys())), ' ')}: {sum([v[key] for v in flat_response])}\n"

    return s


def check_response_failure(response_json):
    """Check the response for failure"""

    if response_json['_shards']['failed'] > 0:
        raise Exception(f"Elasticsearch shards failed: {response_json['_shards']['failures']}")


def main():
    """Used for dev, queries the data for yesterday"""

    monday = datetime.datetime(2025, 10, 1).replace(tzinfo=datetime.timezone(datetime.timedelta(hours=-6)))

    start = monday
    end = start + datetime.timedelta(days=31)

    summary_records = get_ospool_ad_summary(start, end, "http://localhost:9200")


if __name__ == "__main__":
    """Used for debugging and stepping through outputs"""
    main()

print("adstash module loaded")
