import logging
from pathlib import Path

import requests
import yaml
from flask import jsonify

logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


# Get a list of entity uuids via entity-api for a given entity type:
# Collection, Donor, Source, Sample, Dataset, Submission. Case-insensitive.
def get_uuids_by_entity_type(entity_type, request_headers, entity_api_url):
    entity_type = entity_type.lower()

    # url = app.config['ENTITY_API_URL'] + "/" + entity_type + "/entities?property=uuid"
    url = entity_api_url + "/" + entity_type + "/entities?property=uuid"

    response = requests.get(url, headers=request_headers, verify=False)

    if response.status_code != 200:
        return jsonify(error=str(
            "get_uuids_by_entity_type() failed to make a request to entity-api for entity type: " + entity_type)), 500

    uuids_list = response.json()

    return uuids_list


# Gets a list of actually public and private indice names
# Only the indices with `reindex_enabled: true`
def get_all_reindex_enabled_indice_names(all_indices):
    all_names = {}
    try:
        indices = all_indices['indices'].keys()
        for i in indices:
            target_index = all_indices['indices'][i]
            if 'reindex_enabled' in target_index and target_index['reindex_enabled'] is True:
                index_info = {}
                index_names = []
                public_index = target_index['public']
                private_index = target_index['private']
                index_names.append(public_index)
                index_names.append(private_index)
                index_info[i] = index_names
                all_names.update(index_info)
    except Exception as e:
        raise e

    return all_names

def remove_specific_key_entry(obj, key_to_remove=None):
    if type(obj) == dict:
        if key_to_remove in obj.keys():
            obj.pop(key_to_remove)

        for key in obj.keys():
            remove_specific_key_entry(obj[key], key_to_remove)
    elif type(obj) == list:
        for e in obj:
            remove_specific_key_entry(e, key_to_remove)


# To be used by the full index to ensure the nexus token
# belongs to HuBMAP-Data-Admin group
def user_belongs_to_data_admin_group(user_group_ids, data_admin_group_uuid):
    for group_id in user_group_ids:
        if group_id == data_admin_group_uuid:
            return True

    # By now, no match
    return False
