import requests
import functools


@functools.lru_cache(maxsize=1)
def get_institution_id_to_metadata_map():
    institutions = {i['id']: i for i in requests.get("https://topology-institutions.osg-htc.org/api/institution_ids").json()}

    # Add in the institutions modified id's that are found in the MachineAttr in format `osg-htc.org_iid_<hex>`
    for k, v in [*institutions.items()]:
        modified_id = k
        modified_id = modified_id.replace("https://osg-htc.org/iid/", "osg-htc.org_iid_")
        institutions[modified_id] = v

    return institutions
