#!/usr/bin/env python3

from pathlib import Path
from copy import deepcopy
import logging
import sys
from json import dumps
import datetime

# import jsonschema
from yaml import dump as dump_yaml, safe_load as load_yaml

from elasticsearch.addl_index_transformations.portal.translate import (
    translate, TranslationException
)
from elasticsearch.addl_index_transformations.portal.add_everything import (
    add_everything
)


def transform(doc, batch_id='unspecified'):
    '''
    >>> from pprint import pprint
    >>> transformed = transform({
    ...    'entity_type': 'dataset',
    ...    'origin_sample': {
    ...        'organ': 'LY01'
    ...    },
    ...    'create_timestamp': 1575489509656,
    ...    'ancestor_ids': ['1234', '5678'],
    ...    'donor': {
    ...        "metadata": {
    ...             "organ_donor_data": [
    ...                 {
    ...                     "data_type": "Nominal",
    ...                     "grouping_concept_preferred_term":
    ...                         "Gender finding",
    ...                     "preferred_term": "Masculine gender"
    ...                 }
    ...             ]
    ...         }
    ...    }
    ... })
    >>> del transformed['mapper_metadata']['datetime']
    >>> pprint(transformed)
    {'ancestor_ids': ['1234', '5678'],
     'create_timestamp': 1575489509656,
     'donor': {'mapped_metadata': {'gender': 'Masculine gender'},
               'metadata': {'organ_donor_data': [{'data_type': 'Nominal',
                                                  'grouping_concept_preferred_term': 'Gender '
                                                                                     'finding',
                                                  'preferred_term': 'Masculine '
                                                                    'gender'}]}},
     'entity_type': 'dataset',
     'everything': ['1234',
                    '1575489509656',
                    '2019-12-04 19:58:29',
                    '5678',
                    'Gender finding',
                    'LY01',
                    'Lymph Node',
                    'Masculine gender',
                    'Nominal',
                    'dataset'],
     'mapped_create_timestamp': '2019-12-04 19:58:29',
     'mapper_metadata': {'size': 580, 'version': '0.0.1'},
     'origin_sample': {'mapped_organ': 'Lymph Node', 'organ': 'LY01'}}

    '''
    doc_copy = deepcopy(doc)
    # We will modify in place below,
    # so make a deep copy so we don't surprise the caller.
    _clean(doc_copy)
    try:
        translate(doc_copy)
    except TranslationException as e:
        logging.error(f'Batch {batch_id}; UUID {doc["uuid"]}: {e}')
        return None
    add_everything(doc_copy)
    doc_copy['mapper_metadata'] = {
        'version': '0.0.1',
        'datetime': str(datetime.datetime.now()),
        'size': len(dumps(doc_copy))
    }
    return doc_copy


_data_dir = Path(__file__).parent / 'search-schema' / 'data'


def _clean(doc):
    return doc
    # TODO: Reenable.
    # _map(doc, _simple_clean)


def _map(doc, clean):
    # The recursion is usually not needed...
    # but better to do it everywhere than to miss one case.
    clean(doc)
    if 'donor' in doc:
        _map(doc['donor'], clean)
    if 'origin_sample' in doc:
        _map(doc['origin_sample'], clean)
    if 'source_sample' in doc:
        for sample in doc['source_sample']:
            _map(sample, clean)

# TODO: Reenable this when we have time, and can make sure we don't need these fields.
#
# def _simple_clean(doc):
#     schema = _get_schema(doc)
#     allowed_props = schema['properties'].keys()
#     keys = list(doc.keys())
#     for key in keys:
#         if key not in allowed_props:
#             del doc[key]

#     # Not used in portal:
#     for unused_key in [
#         'ancestors',  # ancestor_ids *is* used in portal.
#         'descendants',
#         'descendant_ids',
#         'hubmap_display_id',  # Only used in ingest.
#         'rui_location'
#     ]:
#         if unused_key in doc:
#             del doc[unused_key]


_schemas = {
    entity_type:
        load_yaml((
            _data_dir / 'schemas' / f'{entity_type}.schema.yaml'
        ).read_text())
    for entity_type in ['dataset', 'donor', 'sample']
}


def _get_schema(doc):
    entity_type = doc['entity_type'].lower()
    return _schemas[entity_type]


# TODO:
# def _validate(doc):
#     jsonschema.validate(doc, _get_schema(doc))


if __name__ == "__main__":
    for name in sys.argv[1:]:
        doc = load_yaml(Path(name).read_text())
        transformed = transform(doc)
        print(dump_yaml(transformed))
