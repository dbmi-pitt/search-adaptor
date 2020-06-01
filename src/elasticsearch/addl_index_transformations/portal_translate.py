from pathlib import Path
import re

from yaml import safe_load as load_yaml


def translate(doc):
    _translate_status(doc)
    _translate_organ(doc)
    _translate_donor_metadata(doc)


# Utils:

_data_dir = Path(__file__).parent / 'search-schema' / 'data'


_enums = load_yaml(
        (_data_dir / 'definitions.yaml').read_text()
    )['enums']


def _map(doc, key, map):
    # The recursion is usually not needed...
    # but better to do it everywhere than to miss one case.
    if key in doc:
        doc[key] = map(doc[key])
    if 'donor' in doc:
        _map(doc['donor'], key, map)
    if 'origin_sample' in doc:
        _map(doc['origin_sample'], key, map)
    if 'source_sample' in doc:
        for sample in doc['source_sample']:
            _map(sample, key, map)


# Status:

def _translate_status(doc):
    '''
    >>> doc = {'status': 'NEW'}
    >>> _translate_status(doc); doc
    {'status': 'New'}

    >>> doc = {'status': 'qa'}
    >>> _translate_status(doc); doc
    {'status': 'QA'}

    '''
    _map(doc, 'status', _status_map)


def _status_map(k):
    if k.upper() == 'QA':
        return 'QA'
    description = _status_dict[k]
    return description.title()


_status_dict = {
    k: v['description']
    for k, v in _enums['dataset_status_types'].items()
}


# Organ:

def _translate_organ(doc):
    '''
    >>> doc = {'organ': 'LY01'}
    >>> _translate_organ(doc); doc
    {'organ': 'Lymph Node'}

    >>> doc = {'origin_sample': {'organ': 'RK'}}
    >>> _translate_organ(doc); doc
    {'origin_sample': {'organ': 'Kidney (Right)'}}

    '''
    _map(doc, 'organ', _organ_map)


def _organ_map(k):
    return _organ_dict[k]


_organ_dict = {
    k: re.sub(r'\s+\d+$', '', v['description'])
    for k, v in _enums['organ_types'].items()
}


# Donor metadata:

def _translate_donor_metadata(doc):
    '''
    >>> doc = {
    ...     "metadata": {
    ...         "organ_donor_data": [
    ...             {
    ...                 "data_type": "Nominal",
    ...                 "grouping_concept_preferred_term":
    ...                     "Gender finding",
    ...                 "preferred_term": "Masculine gender",
    ...             },
    ...             {
    ...                 "data_type": "Numeric",
    ...                 "data_value": "58",
    ...                 "grouping_concept_preferred_term":
    ...                     "Current chronological age",
    ...                 "units": "years"
    ...             },
    ...             {
    ...                 "data_type": "Numeric",
    ...                 "data_value": "22",
    ...                 "grouping_concept_preferred_term":
    ...                     "Body mass index",
    ...                 "units": "kg/m^17"
    ...             },
    ...             {
    ...                 "data_type": "Nominal",
    ...                 "grouping_concept_preferred_term":
    ...                     "Racial group",
    ...                 "preferred_term": "African race",
    ...             }
    ...         ]
    ...     }
    ... }
    >>> _translate_donor_metadata(doc)
    >>> from pprint import pprint
    >>> pprint(doc)
    {'metadata': {'age': 58.0,
                  'bmi': 22.0,
                  'gender': 'Masculine gender',
                  'race': 'African race'}}

    '''
    _map(doc, 'metadata', _donor_metadata_map)


def _donor_metadata_map(metadata):
    recognized_terms = {
        'Body mass index': 'bmi',
        'Current chronological age': 'age',
        'Gender finding': 'gender',
        'Racial group': 'race'
    }
    if 'organ_donor_data' in metadata:
        for kv in metadata['organ_donor_data']:
            k = recognized_terms[kv['grouping_concept_preferred_term']]
            v = (
                kv['preferred_term']
                if kv['data_type'] == 'Nominal'
                else float(kv['data_value'])
            )
            metadata[k] = v
        del metadata['organ_donor_data']
    return metadata
