import os
from dataclasses import dataclass, field
from typing import Optional

from flask import abort, Flask, json, Response
import logging
import requests
from urllib.parse import urlparse
from hubmap_commons.S3_worker import S3Worker

logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s:%(lineno)d: %(message)s', level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

app = Flask(__name__, instance_path=os.path.join(os.path.abspath(os.path.dirname(__file__)), 'instance'),
            instance_relative_config=True)


####################################################################################################
## Internal Functions Used By API
####################################################################################################

# Throws error for 400 Bad Reqeust with message
def bad_request_error(err_msg):
    abort(400, description=err_msg)


# Throws error for 401 Unauthorized with message
def unauthorized_error(err_msg):
    abort(401, description=err_msg)


# Throws error for 403 Forbidden with message
def forbidden_error(err_msg):
    abort(403, description=err_msg)


# Throws error for 500 Internal Server Error with message
def internal_server_error(err_msg):
    abort(500, description=err_msg)


def get_uuids_from_es(index, es_url):
    uuids = []
    size = 10_000
    query = {
        "size": size,
        "from": len(uuids),
        "_source": ["_id"],
        "query": {
            "bool": {
                "must": [],
                "filter": [
                    {
                        "match_all": {}
                    }
                ],
                "should": [],
                "must_not": []
            }
        }
    }

    end_of_list = False
    while not end_of_list:
        logger.debug("Searching ES for uuids...")
        logger.debug(es_url)

        resp = execute_query('_search', None, index, es_url, query)

        logger.debug(f"OpenSearch '_search' query response returned resp.status_code={resp.status_code}.")
        # @TODO-If a 303 response is returned, retrieve the JSON from an AWS bucket (not currently anticipated to happen.)

        ret_obj = resp.get_json()
        uuids.extend(hit['_id'] for hit in ret_obj.get('hits').get('hits'))

        total = ret_obj.get('hits').get('total').get('value')
        if total <= len(uuids):
            end_of_list = True
        else:
            query['from'] = len(uuids)

    return uuids

def execute_opensearch_query(query_against, request, index, es_url, query=None, request_params=None):
    supported_query_against = ['_search', '_count', '_mget']
    supported_endpoints_with_id = ['_update']
    supported_endpoints = supported_query_against + supported_endpoints_with_id
    separator = ','

    # If query_against has a / in it, assume a 32 character identifier after the / is okay, but
    # verify the endpoint_base name before the / is in supported_query_against
    if '/' in query_against:
        endpoint_elements = query_against.split(sep='/'
                                                ,maxsplit=1)
        # For an internal function like this, assume the 32 character part after the / is
        # a UUID without verifying the format, and allow it through.
        if  endpoint_elements[0] not in supported_endpoints_with_id \
            or len(endpoint_elements[1]) != 32:
            bad_request_error(f"Query of endpoint '{endpoint_elements[0]}'"
                              f" with identifier '{endpoint_elements[1]}'"
                              " is not supported by Search API."
                              f" Supported endoints are: {separator.join(supported_endpoints)}")
    elif query_against not in supported_query_against:
        bad_request_error(  f"Query against '{query_against}' is not supported by Search API."
                            f" Use one of the following: {separator.join(supported_endpoints)}")

    # Determine the target real index in Elasticsearch to be searched against
    # index = get_target_index(request, index_without_prefix)

    # target_url = app.config['ELASTICSEARCH_URL'] + '/' + target_index + '/' + query_against
    # es_url = INDICES['indices'][index_without_prefix]['elasticsearch']['url'].strip('/')

    logger.debug('es_url')
    logger.debug(es_url)
    logger.debug(type(es_url))
    # use the index es connection
    target_url = es_url + '/' + index + '/' + query_against
    if request_params:
        target_url = target_url + '?'
        for param in request_params.keys():
            target_url = target_url + param + '=' + request_params[param] + '&'
        # dump the last ampersand
        target_url = target_url[:-1]

    logger.debug("Target url: " + target_url)
    if query is None:
        # Parse incoming json string into json data(python dict object)
        json_data = request.get_json()

        # All we need to do is to simply pass the search json to elasticsearch
        # The request json may contain "access_group" in this case
        # Will also pass through the query string in URL
        target_url = target_url + get_query_string(request.url)
        # Make a request with json data
        # The use of json parameter converts python dict to json string and adds content-type: application/json automatically
    else:
        json_data = query

    return requests.post(url=target_url, json=json_data)

def size_response_for_gateway(response_json=None, large_response_settings_dict=None):

    if large_response_settings_dict is not None:
        # Since the calling service passed in a dictionary of settings for AWS S3, stash
        # any large responses there.  Otherwise, allow the response to be returned directly
        # as this function exits.
        if len(response_json.encode('utf-8')) >= large_response_settings_dict['large_response_threshold']:
            anS3Worker = None
            try:
                anS3Worker = S3Worker(  large_response_settings_dict['aws_access_key_id']
                                        ,large_response_settings_dict['aws_secret_access_key']
                                        ,large_response_settings_dict['aws_s3_bucket_name']
                                        ,large_response_settings_dict['aws_object_url_expiration_in_secs'])
                logger.info("anS3Worker initialized")
                obj_key = anS3Worker.stash_text_as_object(  response_json
                                                            ,large_response_settings_dict['service_configured_obj_prefix'])
                aws_presigned_url = anS3Worker.create_URL_for_object(obj_key)
                return Response(    response=aws_presigned_url
                                    , status=303) # See Other
            except Exception as s3exception:
                logger.error(   f"Error getting anS3Worker to handle len(results)="
                                f"{len(response_json.encode('utf-8'))}.")
                logger.error(s3exception, exc_info=True)
                return Response(    response=f"Unexpected error storing large results in S3. See logs."
                                    ,status=500)
    else:
        # Since the calling service did not pass in a dictionary of settings for AWS S3, execute the
        # traditional handling to check for responses over 10MB and return more useful message instead
        # of AWS API Gateway's default 500 message.
        # Note Content-length header is not always provided, we have to calculate
        check_response_payload_size(response_json)
    return None

def execute_query(query_against, request, index, es_url, query=None, request_params=None, large_response_settings_dict=None):
    opensearch_response = execute_opensearch_query(query_against=query_against
                                                   ,request=request
                                                   ,index=index
                                                   ,es_url=es_url
                                                   ,query=query
                                                   ,request_params=request_params)

    # Continue on using the exact JSON returned by the OpenSearch query. Use cases which need to
    # manipulate the JSON for their response should do their own execute_opensearch_query() and
    # size_response_for_gateway(), with the manipulation between the calls.

    s3_response = size_response_for_gateway(response_json=json.dumps(opensearch_response.json())
                                            , large_response_settings_dict=large_response_settings_dict)
    if s3_response is not None:
        return s3_response
    else:
        # Convert the requests.models.Response to a flask.wrappers.Response to
        # return results through the view.
        return Response(response=json.dumps(opensearch_response.json())
                        , status=opensearch_response.status_code
                        , mimetype='application/json')

# Get the query string from orignal request
def get_query_string(url):
    query_string = ''
    parsed_url = urlparse(url)

    logger.debug("======parsed_url======")
    logger.debug(parsed_url)

    # Add the ? at beginning of the query string if not empty
    if not parsed_url.query:
        query_string = '?' + parsed_url.query

    return query_string


"""
Send back useful error message instead of AWS API Gateway's default 500 message
when the response payload size is over 10MB (10485760 bytes)

Parameters
----------
response_text: str
    The http response body string

Returns
-------
flask.Response
    500 response with error message if over the hard limit
"""


def check_response_payload_size(response_text):
    search_result_payload = len(response_text.encode('utf-8'))
    aws_api_gateway_payload_max = 10485760

    if search_result_payload > aws_api_gateway_payload_max:
        msg = f'Search result length {search_result_payload} is larger than allowed maximum of {aws_api_gateway_payload_max} bytes'
        logger.debug(msg)
        internal_server_error(msg)


def upsert(doc: dict, index: str, es_url: str, headers: Optional[dict] = None, verify: bool = False):
    """ Update or insert a document in the index.

    Parameters
    ----------
    doc: dict
        The document to be updated or inserted.
    index: str
        The index where the document is to be updated or inserted.
    es_url: str
        The URL of the Elasticsearch or OpenSearch instance.
    headers: dict, optional
        The headers to be included in the request.
    verify: bool, optional
        Whether to verify the SSL certificate.

    Returns
    -------
    requests.Response
        The response from the Elasticsearch or OpenSearch instance.
    """
    url = f"{es_url}/{index}/_update/{doc['uuid']}"
    body = {
        "doc": doc,
        "doc_as_upsert": True
    }
    return requests.post(url, json=body, headers=headers, verify=verify)


@dataclass(frozen=True)
class BulkUpdate:
    """A class to represent a bulk update operation.

    Attributes
    ----------
    upserts: list[dict]
        The documents to be updated or inserted.
    deletes: list[str]
        The UUIDs of the documents to be deleted.
    """
    upserts: list[dict] = field(default_factory=list)
    deletes: list[str] = field(default_factory=list)


def bulk_update(bulk_update: BulkUpdate, index: str, es_url: str, headers: Optional[dict] = None, verify: bool = False):
    """Upsert (update or insert) or delete multiple documents in the index.

    Parameters
    ----------
    bulk_update: BulkUpdate
        The bulk update object containing the documents to be updated or inserted and the UUIDs of the documents to be deleted.
    index: str
        The index where the documents are to be updated or inserted or deleted.
    es_url: str
        The URL of the Elasticsearch or OpenSearch instance.
    headers: dict, optional
        The headers to be included in the request.
    verify: bool, optional
        Whether to verify the SSL certificate.

    Returns
    -------
    requests.Response
        The response from the Elasticsearch or OpenSearch instance.

    Raises
    ------
    ValueException
        If no upserts or deletes are provided.
    """
    if not bulk_update.upserts and not bulk_update.deletes:
        return ValueError("No upserts or deletes provided.")

    url = f"{es_url}/{index}/_bulk"
    if headers is None:
        headers = {"Content-Type": "application/x-ndjson"}
    else:
        headers["Content-Type"] = "application/x-ndjson"

    # Preparing ndjson content
    upserts = [
        f'{{"update":{{"_id":"{upsert["uuid"]}"}}}}\n{{"doc":{json.dumps(upsert, separators=(",", ":"))},"doc_as_upsert":true}}'
        for upsert in bulk_update.upserts
    ]
    deletes = [f'{{ "delete": {{ "_id": "{delete_uuid}" }} }}' for delete_uuid in bulk_update.deletes]

    body = "\n".join(upserts + deletes) + "\n"
    return requests.post(url, headers=headers, data=body, verify=verify)
