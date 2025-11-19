"""
Generates mapped summary records for the OSPool
"""
import datetime
import logging
from collections import defaultdict

import pandas as pd

from summarize.field_of_science import FieldOfScienceMapper
from summarize.adstash import get_ospool_ad_summary
from summarize.institution_api import get_institution_id_to_metadata_map
from summarize.topology import get_resource_to_institution_id_map, get_acct_group_to_project_metadata_map, get_resource_group_to_institution_id_map

# Configure logging
logger = logging.getLogger(__name__)


def get_summary_records(start: datetime.datetime = None, end: datetime.datetime = None, host: str = None):
    """Get the summary records for a single day, defaults to span UTC yesterday"""

    # If start is None, set the range to span yesterday
    if start is None:
        end = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=datetime.timezone.utc)
        start = end - datetime.timedelta(days=1)

    # If end is None, set it to the start + 1 day
    if end is None:
        end = start + datetime.timedelta(days=1)

    # Set up the mappers
    acct_group_to_metadata_map = get_acct_group_to_project_metadata_map()
    institution_id_to_metadata_map = get_institution_id_to_metadata_map()
    fos_mapper = FieldOfScienceMapper()

    ospool_ad_summary = get_ospool_ad_summary(start=start, end=end, host=host)

    summary_records = []
    for summary_record in ospool_ad_summary:
        resource = summary_record['ResourceName']
        acct_group = summary_record['AcctGroup']

        broad_field_of_science, major_field_of_science, detailed_field_of_science = fos_mapper.map_id_to_fields_of_science(
            acct_group_to_metadata_map.get(acct_group.lower(), {}).get('FieldOfScienceID', None)
        )
        project_institution = institution_id_to_metadata_map.get(acct_group_to_metadata_map.get(acct_group.lower(), {}).get('InstitutionID', None), None)
        resource_institution = get_resource_institution(summary_record)

        summary_records.append({
            "ProjectInstitution": project_institution,
            "ResourceInstitution": resource_institution,
            "ResourceInstitutionID": resource_institution['id'] if resource_institution is not None else None,
            'ResourceName': resource,
            'ProjectName': acct_group,
            'BroadFieldOfScience': broad_field_of_science,
            'MajorFieldOfScience': major_field_of_science,
            'DetailedFieldOfScience': detailed_field_of_science,
            'NumJobs': summary_record['NumJobs'],
            'CpuHours': summary_record['CpuHours'],
            'GpuHours': summary_record['GpuHours'],
            'OSDFFileTransferCount': summary_record['OSDFFileTransferCount'],
            'OSDFByteTransferCount': summary_record['OSDFByteTransferCount'],
            'FileTransferCount': summary_record['FileTransferCount'],
            'ByteTransferCount': summary_record['ByteTransferCount'],
            'isNRP': summary_record['isNRP'],
            'Date': str(start.date())
        })

    return summary_records


def get_resource_institution(record: dict):
    """Find the matching institution ID and subsequent metadata for the given record"""

    # If the record has an institution ID, use that
    if 'InstitutionID' in record and record['InstitutionID'] != "UNKNOWN":
        logger.debug(f"Resource {record['ResourceName']} has an InstitutionID")

        institution = get_institution_id_to_metadata_map().get(record['InstitutionID'], None)

        logger.debug(f"Resource {record['ResourceName']} has InstitutionID {record['InstitutionID']}")

        return institution

    # If the record has a resource name, use that
    if record['ResourceName'].lower() in get_resource_to_metadata_map():
        return get_resource_to_metadata_map()[record['ResourceName'].lower()]

    # It isn't odd for a 'ResourceName' to be a resource group, so check that too
    if record['ResourceName'].lower() in get_resource_group_to_metadata_map():
        return get_resource_group_to_metadata_map()[record['ResourceName'].lower()]

    return None


def get_resource_to_metadata_map():
    resource_to_institution_id_map = get_resource_to_institution_id_map()
    institution_id_to_metadata_map = get_institution_id_to_metadata_map()

    return {resource.lower(): institution_id_to_metadata_map.get(institution_id, None) for resource, institution_id in resource_to_institution_id_map.items()}


def get_resource_group_to_metadata_map():
    resource_group_to_institution_id_map = get_resource_group_to_institution_id_map()
    institution_id_to_metadata_map = get_institution_id_to_metadata_map()

    return {resource_group.lower(): institution_id_to_metadata_map.get(institution_id, None) for resource_group, institution_id in resource_group_to_institution_id_map.items()}


def main():
    """Used for dev, queries the data for yesterday"""

    monday = datetime.datetime(2025, 4, 27).replace(tzinfo=datetime.timezone(datetime.timedelta(hours=-6)))

    start = monday
    end = start + datetime.timedelta(days=1)

    summary_records = get_summary_records(start, end)


if __name__ == "__main__":
    """Used for debugging and stepping through outputs"""
    main()