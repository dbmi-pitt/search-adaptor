import os

from flask import abort, jsonify, Flask, json, Response
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
    separator = ','

    if query_against not in supported_query_against:
        bad_request_error(
            f"Query against '{query_against}' is not supported by Search API. Use one of the following: {separator.join(supported_query_against)}")

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

    logger.debug(json_data)

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
