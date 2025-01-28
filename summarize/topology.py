import requests
import functools


@functools.lru_cache
def get_resource_to_institution_id_map():
    resources = requests.get("https://topology.opensciencegrid.org/miscresource/json").json()
    facilities = requests.get("https://topology.opensciencegrid.org/miscfacility/json").json()

    return {r['Name'].lower(): facilities[r['Facility']]['InstitutionID'] for r in resources.values()}


@functools.lru_cache
def get_resource_group_to_institution_id_map():
    resources = requests.get("https://topology.opensciencegrid.org/miscresource/json").json()
    facilities = requests.get("https://topology.opensciencegrid.org/miscfacility/json").json()

    return {r['ResourceGroup'].lower(): facilities[r['Facility']]['InstitutionID'] for r in resources.values()}


@functools.lru_cache
def get_acct_group_to_project_metadata_map():
    acct_groups = requests.get("https://topology.opensciencegrid.org/miscproject/json").json()

    return {k.lower(): v for k, v in acct_groups.items()}
