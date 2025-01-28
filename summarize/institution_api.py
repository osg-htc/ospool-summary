import requests
import functools


@functools.lru_cache(maxsize=1)
def get_institution_id_to_metadata_map():
    return {i['id']: i for i in requests.get("https://topology-institutions.osg-htc.org/api/institution_ids").json()}
