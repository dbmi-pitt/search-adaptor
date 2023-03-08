import concurrent.futures
import inspect
import threading
from pathlib import Path
import json

from flask import request, Response
# HuBMAP commons
from hubmap_commons.hm_auth import AuthHelper
from urllib3.exceptions import InsecureRequestWarning
from yaml import safe_load

# Local modules
from opensearch_helper_functions import *

# Suppress InsecureRequestWarning warning when requesting status on https with ssl cert verify disabled
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

# Set logging format and level (default is warning)
# All the API logging is forwarded to the uWSGI server and gets written into the log file `uwsgo-entity-api.log`
# Log rotation is handled via logrotate on the host system with a configuration file
# Do NOT handle log file and rotation via the Python logging to avoid issues with multi-worker processes

logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s:%(lineno)d: %(message)s', level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')

class SearchAPI:
    def __init__(self, config, translator_module, blueprint=None):
        # Set self based on passed in config parameters
        for key, value in config.items():
            setattr(self, key, value)

        self.translator_module = translator_module

        self.S3_settings_dict = {   'large_response_threshold': self.LARGE_RESPONSE_THRESHOLD
                                    ,'aws_access_key_id': self.AWS_ACCESS_KEY_ID
                                    ,'aws_secret_access_key': self.AWS_SECRET_ACCESS_KEY
                                    ,'aws_s3_bucket_name': self.AWS_S3_BUCKET_NAME
                                    ,'aws_object_url_expiration_in_secs': self.AWS_OBJECT_URL_EXPIRATION_IN_SECS
                                    ,'service_configured_obj_prefix': self.AWS_S3_OBJECT_PREFIX}

        # Specify the absolute path of the instance folder and use the config file relative to the instance path
        self.app = Flask(__name__, instance_path=os.path.join(os.path.abspath(os.path.dirname(__file__))))
        if blueprint is not None:
            self.app.register_blueprint(blueprint)

        @self.app.errorhandler(400)
        def __http_bad_request(e):
            return self.http_bad_request(e)

        @self.app.errorhandler(401)
        def __http_unauthorized(e):
            return self.http_unauthorized(e)

        @self.app.errorhandler(403)
        def __http_forbidden(e):
            return self.http_forbidden(e)

        @self.app.errorhandler(500)
        def __http_internal_server_error(e):
            return self.http_internal_server_error(e)

        @self.app.route('/', methods=['GET'])
        def __index():
            return self.index()

        @self.app.route('/search', methods=['POST'])
        def __search():
            return self.search()

        @self.app.route('/<index_without_prefix>/search', methods=['POST'])
        def __search_by_index(index_without_prefix):
            return self.search_by_index(index_without_prefix)

        @self.app.route('/mget', methods=['POST'])
        def __mget():
            return self.mget()

        @self.app.route('/<index_without_prefix>/mget', methods=['POST'])
        def __mget_by_index(index_without_prefix):
            return self.mget_by_index(index_without_prefix)

        @self.app.route('/count', methods=['GET'])
        def __count():
            return self.count()

        @self.app.route('/<index_without_prefix>/count', methods=['GET'])
        def __count_by_index(index_without_prefix):
            return self.count_by_index(index_without_prefix)

        @self.app.route('/indices', methods=['GET'])
        def __indices():
            return self.indices()

        @self.app.route('/status', methods=['GET'])
        def __status():
            return self.status()

        @self.app.route('/reindex/<uuid>', methods=['PUT'])
        def __reindex(uuid):
            return self.reindex(uuid)

        @self.app.route('/reindex-all', methods=['PUT'])
        def __reindex_all():
            return self.reindex_all()

        @self.app.route('/update/<uuid>', methods=['PUT'])
        @self.app.route('/update/<uuid>/<index>', methods=['PUT'])
        @self.app.route('/update/<uuid>/<index>/<scope>', methods=['PUT'])
        def __update(uuid, index=None, scope=None):
            return self.update(uuid, index, scope)

        @self.app.route('/add/<uuid>', methods=['POST'])
        @self.app.route('/add/<uuid>/<index>', methods=['POST'])
        @self.app.route('/add/<uuid>/<index>/<scope>', methods=['POST'])
        def __add(uuid, index=None, scope=None):
            return self.add(uuid, index, scope)

        @self.app.route('/clear-docs/<index>', methods=['POST'])
        @self.app.route('/clear-docs/<index>/<uuid>', methods=['POST'])
        @self.app.route('/clear-docs/<index>/<uuid>/<scope>', methods=['POST'])
        def __clear_docs(index, uuid=None, scope=None):
            return self.clear_docs(index=index, scope=scope, uuid=uuid)

        @self.app.route('/<index>/scroll-search', methods=['POST'])
        # This may be a non-AWS Gateway endpoint, initially used by files-api,
        # possibly (@TODO) returning more than 10Mb, and possibly in over 30 secs.
        def __scroll_search(index):
            try:
                return self.scrollsearch_index(composite_index=index)
            except requests.HTTPError as he:
                # OpenSearch errors come back in the JSON, so return them that way.
                return jsonify(he.response.json()), he.response.status_code

        ####################################################################################################
        ## AuthHelper initialization
        ####################################################################################################

        # Initialize AuthHelper class and ensure singleton
        try:
            if AuthHelper.isInitialized() == False:
                self.auth_helper_instance = AuthHelper.create(self.APP_CLIENT_ID, self.APP_CLIENT_SECRET)
                logger.info("Initialized AuthHelper class successfully :)")
            else:
                self.auth_helper_instance = AuthHelper.instance()
        except Exception:
            msg = "Failed to initialize the AuthHelper class"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    ####################################################################################################
    ## Register error handlers
    ####################################################################################################

    # Error handler for 400 Bad Request with custom error message
    def http_bad_request(self, e):
        return jsonify(error=str(e)), 400

    # Error handler for 401 Unauthorized with custom error message
    def http_unauthorized(self, e):
        return jsonify(error=str(e)), 401

    # Error handler for 403 Forbidden with custom error message
    def http_forbidden(self, e):
        return jsonify(error=str(e)), 403

    # Error handler for 500 Internal Server Error with custom error message
    def http_internal_server_error(self, e):
        return jsonify(error=str(e)), 500

    ####################################################################################################
    ## Default route
    ####################################################################################################

    def index(self):
        return "Hello! This is the Search API service :)"

    ####################################################################################################
    ## API
    ####################################################################################################

    # Both HTTP GET and HTTP POST can be used to execute search with body against ElasticSearch REST API.
    # BUT AWS API Gateway only supports POST with request body
    # general search uses the DEFAULT_INDEX
    def search(self):
        # Always expect a json body
        self.request_json_required(request)

        logger.info("======search with no index provided======")
        logger.info("default_index: " + self.DEFAULT_INDEX_WITHOUT_PREFIX)

        # Determine the target real index in Elasticsearch to be searched against
        # Use the DEFAULT_INDEX_WITHOUT_PREFIX since /search doesn't take any index
        target_index = self.get_target_index(request, self.DEFAULT_INDEX_WITHOUT_PREFIX)

        # get URL for that index
        es_url = self.INDICES['indices'][self.DEFAULT_INDEX_WITHOUT_PREFIX]['elasticsearch']['url'].strip(
            '/')

        # Set a prefix used for naming any objects that end up in S3 which is
        # specific to this service and this function.
        function_name = inspect.currentframe().f_code.co_name
        self.S3_settings_dict['service_configured_obj_prefix'] = \
            f"{self.AWS_S3_OBJECT_PREFIX.replace('unspecified-function',function_name)}"

        response = execute_query(   query_against='_search'
                                    ,request=request
                                    ,index=target_index
                                    ,es_url=es_url
                                    ,query=None
                                    ,large_response_settings_dict=self.S3_settings_dict)

        return response

    # Verify "modify" permissions for a specified Dataset for the token presented.
    def _verify_dataset_permission(self, dataset_uuid, token, translator):
        Dataset = translator.call_entity_api(dataset_uuid, 'entities')
        dataset_group_uuid = Dataset['group_uuid']
        user_groups_by_id_dict = self.auth_helper_instance.get_globus_groups_info()['by_id']
        if not dataset_group_uuid in user_groups_by_id_dict.keys() and \
           not self.auth_helper_instance.has_data_admin_privs(token):
            bad_request_error(f"Permission denied for modifying index entries for '{dataset_uuid}'.")

    def _open_scroll(self, target_index, scroll_open_minutes, oss_base_url, json_dict):
        logger.debug(f"_open_scroll for target_index={target_index}, scroll_open_minutes={scroll_open_minutes}")

        # Form a URL which passes scroll as a parameter, with a string value, to create a search scroll.
        target_url = f"{oss_base_url}/{target_index}/_search?scroll={scroll_open_minutes}m"
        logger.debug(f"Opening scroll using target_url={target_url}")

        response = requests.post(target_url, json=json_dict)

        if response.status_code == 200:
            logger.debug(f"Scroll opened with _scroll_id='{response.json()['_scroll_id']}'")
            logger.debug(f"during open, {len(response.json()['hits']['hits'])} returned.")
        else:
            raise requests.exceptions.HTTPError(response=response)
            return f"Scroll open failed, with status_code={response.status_code}, {response.reason}.", response.reason

        # After a successful call, check if the response payload exceeds the size that
        # can pass through the AWS Gateway.
        if response.status_code == 200:
           check_response_payload_size(response.text)

        # Return the Elasticsearch resulting json data and status code
        if response.status_code == 200:
            return jsonify(response.json()), response.status_code
        else:
            return f"Scroll open response  is {response.status_code}, {response.reason}.", response.status_code

    def _read_scroll(self, scroll_open_minutes, oss_base_url, json_dict):
        # Extend the scroll open time if there is not one specified in the JSON payload.
        # N.B. This specification of an extended scroll open time in JSON is not interchangeable with
        #      specification the initial scroll open time as a parameter by _open_scroll()
        if not 'scroll' in json_dict:
            json_dict['scroll'] = f"{scroll_open_minutes}m"
        # Expect the caller of this private method to have raised an exception if it
        # could not provide scroll_id in json_dict.
        scroll_id = json_dict['scroll_id'] if 'scroll_id' in json_dict else None
        logger.debug(f"_read_scroll with scroll_id='{scroll_id}'")

        # Form a URL which passes scroll as a parameter, with a string value, to create a search scroll.
        target_url = f"{oss_base_url}/_search/scroll"
        logger.debug(f"Opening scroll using target_url={target_url}")

        response = requests.post(target_url, json=json_dict)

        if response.status_code == 200:
            logger.debug(f"Scroll read with _scroll_id='{response.json()['_scroll_id']}'")
            logger.debug(f"during read, {len(response.json()['hits']['hits'])} returned.")
        else:
            return f"Scroll read failed, with status_code={response.status_code}, {response.reason}.", response.reason

        # After a successful call, check if the response payload exceeds the size that
        # can pass through the AWS Gateway.
        if response.status_code == 200:
           check_response_payload_size(response.text)

        # Return the Elasticsearch resulting json data and status code
        if response.status_code == 200:
            return jsonify(response.json()), response.status_code
        else:
            return f"Scroll read response  is {response.status_code}, {response.reason}.", response.status_code

    def _close_scroll(self, scroll_id, oss_base_url):
        target_url = f"{oss_base_url}/_search/scroll/{scroll_id}"
        logger.debug(f"Closing scroll using target_url={target_url}")
        response = requests.delete(target_url)
        if response.status_code == 200:
            return "Scroll deleted", response.status_code
        else:
            return f"Scroll delete response  is {response.status_code}, {response.reason}.", response.status_code

    # Use the OpenSearch scroll API (https://opensearch.org/docs/latest/api-reference/scroll/)
    # to open a scroll which will be navigated in the specified time when scroll_id is not
    # provided, retrieve more results when a scroll_id is provided, and close a scroll when
    # zero is the specified time.
    #
    # This method works with a composite index name from search-config.yaml.  The actual OpenSearch
    # index searched is determined by get_target_index() from the token presented and composite_index argument.
    def scrollsearch_index(self, composite_index):

        # Always expect a json body, which should at least contain 'scroll_open_minutes'
        self.request_json_required(request)
        json_args = request.get_json()

        # Capture the open scroll context time in minutes from a JSON value custom to this endpoint.
        scroll_open_minutes = json_args['scroll_open_minutes'] if 'scroll_open_minutes' in json_args else None
        # Remove scroll_open_minutes from json_args.  When needed to form a parameter recognized by the
        # OpenSearch API to open a scroll, or to form the 'scroll' JSON entry for reading a scroll, use
        # the scroll_open_minutes value with 'm' appended.
        #
        # Return default of None so do not get a KeyError if the key is not in the dictionary.
        json_args.pop('scroll_open_minutes', None)

        # scroll_open_minutes is required for each operation with the scroll, so
        # verify acceptable number on each request.
        if scroll_open_minutes is not None and (not isinstance(scroll_open_minutes, int) or scroll_open_minutes < 0):
            logger.error(f"Unable to recognize scroll_open_minutes={scroll_open_minutes} as positive integer.")
            bad_request_error(f"Unable to recognize scroll_open_minutes={scroll_open_minutes} as positive integer.")

        scroll_id = json_args['scroll_id'] if 'scroll_id' in json_args else None

        if composite_index not in self.INDICES['indices']:
            msg = f"'{composite_index}' not a configured index."
            logger.error(msg)
            bad_request_error(msg)
        configured_base_url = self.INDICES['indices'][composite_index]['elasticsearch']['url'].strip('/')

        # Deletion of the scroll is indicated by zero minutes on the request
        if scroll_open_minutes == 0:
            if scroll_id is None:
                logger.error(f"scroll_id ={scroll_id} when scroll_open_minutes={scroll_open_minutes} indicates scroll delete operation.")
                bad_request_error("Missing scroll_id for scroll delete operation.")
            else:
                return self._close_scroll(scroll_id, configured_base_url)

        # Continuing to read from an open scroll is indicated by the presence of scroll_id and
        # a non-zero scroll_open_minutes
        if scroll_id is not None and scroll_open_minutes > 0:
            return self._read_scroll(scroll_open_minutes, configured_base_url, json_args)

        # # Open a scroll for the OpenSearch index as indicated by the absence of scroll_id,
        # # a non-zero scroll_open_minutes, and a specification of the scope of the index to use.
        target_index = self.get_target_index(request, composite_index)
        if scroll_id is None and scroll_open_minutes > 0:
            return self._open_scroll(target_index, scroll_open_minutes, configured_base_url, json_args)

        # If this point is reached, no recognized operation was done.
        logger.error(f"Unable to determine operation when"
                        f" target_index={target_index},"
                        f" scroll_id={scroll_id}, and"
                        f" scroll_open_minutes={scroll_open_minutes}.")
        bad_request_error("Unable to work with scroll given parameters.")

    # Both HTTP GET and HTTP POST can be used to execute search with body against ElasticSearch REST API.
    # BUT AWS API Gateway only supports POST with request body
    # Note: the index in URL is not he real index in Elasticsearch, it's that index without prefix
    def search_by_index(self, index_without_prefix):
        # Always expect a json body
        self.request_json_required(request)

        # Make sure the requested index in URL is valid
        self.validate_index(index_without_prefix)

        logger.info("======requested index_without_prefix======")
        logger.info(index_without_prefix)

        # Determine the target real index in Elasticsearch to be searched against
        target_index = self.get_target_index(request, index_without_prefix)

        # get URL for that index
        es_url = self.INDICES['indices'][index_without_prefix]['elasticsearch']['url'].strip('/')

        # Set a prefix used for naming any objects that end up in S3 which is
        # specific to this service and this function.
        function_name = inspect.currentframe().f_code.co_name
        self.S3_settings_dict['service_configured_obj_prefix'] = \
            f"{self.AWS_S3_OBJECT_PREFIX.replace('unspecified-function',function_name)}"

        # Return the elasticsearch resulting json data as json string
        response = execute_query(   query_against='_search'
                                    ,request=request
                                    ,index=target_index
                                    ,es_url=es_url
                                    ,query=None
                                    ,large_response_settings_dict=self.S3_settings_dict)
        return response

    # Info
    def mget(self):
        # Always expect a json body
        self.request_json_required(request)

        logger.info("======mget with no index provided======")
        logger.info("default_index: " + self.DEFAULT_INDEX_WITHOUT_PREFIX)

        if 'docs' in request.get_json():
            for item in request.get_json()['docs']:
                if '_index' in item:
                    bad_request_error(
                        "Index may not be specified in request body. To target a specific index, use /<index>/mget")

        # Determine the target real index in Elasticsearch to be searched against
        # Use the DEFAULT_INDEX_WITHOUT_PREFIX since /search doesn't take any index
        target_index = self.get_target_index(request, self.DEFAULT_INDEX_WITHOUT_PREFIX)

        # get URL for that index
        es_url = self.INDICES['indices'][self.DEFAULT_INDEX_WITHOUT_PREFIX]['elasticsearch']['url'].strip(
            '/')

        # Return the elasticsearch resulting json data as json string
        return execute_query('_mget', request, target_index, es_url)

    # Info
    def mget_by_index(self, index_without_prefix):
        # Always expect a json body
        self.request_json_required(request)

        # Make sure the requested index in URL is valid
        self.validate_index(index_without_prefix)

        logger.info("======requested index_without_prefix======")
        logger.info(index_without_prefix)

        if 'docs' in request.get_json():
            for item in request.get_json()['docs']:
                if '_index' in item:
                    bad_request_error("Index may not be specified in request body. To target a specific index, use /<index>/mgett")

        # Determine the target real index in Elasticsearch to be searched against
        target_index = self.get_target_index(request, index_without_prefix)

        # get URL for that index
        es_url = self.INDICES['indices'][index_without_prefix]['elasticsearch']['url'].strip('/')

        # Return the elasticsearch resulting json data as json string
        return execute_query('_mget', request, target_index, es_url)

    # HTTP GET can be used to execute search with body against ElasticSearch REST API.
    def count(self):
        # Always expect a json body
        self.request_json_required(request)

        logger.info("======count with no index provided======")

        # Determine the target real index in Elasticsearch to be searched against
        target_index = self.get_target_index(request, self.DEFAULT_INDEX_WITHOUT_PREFIX)

        # get URL for that index
        es_url = self.INDICES['indices'][self.DEFAULT_INDEX_WITHOUT_PREFIX]['elasticsearch']['url'].strip('/')

        # Return the elasticsearch resulting json data as json string
        return execute_query('_count', request, target_index, es_url)

    # HTTP GET can be used to execute search with body against ElasticSearch REST API.
    # Note: the index in URL is not he real index in Elasticsearch, it's that index without prefix
    def count_by_index(self, index_without_prefix):
        # Always expect a json body
        self.request_json_required(request)

        # Make sure the requested index in URL is valid
        self.validate_index(index_without_prefix)

        logger.info("======requested index_without_prefix======")
        logger.info(index_without_prefix)

        # Determine the target real index in Elasticsearch to be searched against
        target_index = self.get_target_index(request, index_without_prefix)

        # get URL for that index
        es_url = self.INDICES['indices'][index_without_prefix]['elasticsearch']['url'].strip('/')

        # Return the elasticsearch resulting json data as json string
        return execute_query('_count', request, target_index, es_url)

    # Get a list of indices
    def indices(self):
        # Return the resulting json data as json string
        result = {
            "indices": self.get_filtered_indices()
        }

        return jsonify(result)

    # Get the status of Elasticsearch cluster by calling the health API
    # This shows the connection status and the cluster health status (if connected)
    def status(self):
        response_data = {
            # Use strip() to remove leading and trailing spaces, newlines, and tabs
            'version': ((Path(__file__).absolute().parent.parent.parent.parent / 'VERSION').read_text()).strip(),
            'build': ((Path(__file__).absolute().parent.parent.parent.parent / 'BUILD').read_text()).strip(),
            'elasticsearch_connection': False
        }

        target_url = self.DEFAULT_ELASTICSEARCH_URL + '/_cluster/health'
        resp = requests.get(url=target_url)

        if resp.status_code == 200:
            response_data['elasticsearch_connection'] = True

            # If connected, we also get the cluster health status
            status_dict = resp.json()
            # Add new key
            response_data['elasticsearch_status'] = status_dict['status']

        return jsonify(response_data)

    # This reindex function will also reindex Collection and Upload
    # in addition to the Dataset, Donor, Sample entities
    def reindex(self, uuid):
        # Reindex individual document doesn't require the token to belong
        # to the Data Admin group
        # since this is being used by entity-api and ingest-api too
        token = self.get_user_token(request.headers)

        # Check if query parameter is passed to used futures instead of threading
        asynchronous = request.args.get('async')

        translator = self.init_translator(token)
        if asynchronous:
            try:
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(translator.translate, uuid)
                    result = future.result()
            except Exception as e:
                logger.exception(e)
                internal_server_error(e)

            return result, 202

        else:
            try:
                threading.Thread(target=translator.translate, args=[uuid]).start()

                logger.info(f"Started to update document with uuid: {uuid}")
            except Exception as e:
                logger.exception(e)
                internal_server_error(e)

            return f"Request of reindexing {uuid} accepted", 202

    # Live reindex without first deleting and recreating the indices
    # This just deletes the old document and add the latest document of each entity (if still available)
    def reindex_all(self):
        # The token needs to belong to the Data Admin group
        # to be able to trigger a live reindex for all documents
        token = self.get_user_token(request.headers, admin_access_required=True)
        saved_request = request.headers

        logger.debug(saved_request)

        try:
            translator = self.init_translator(token)
            threading.Thread(target=translator.translate_all, args=[]).start()

            logger.info('Started live reindex all')
        except Exception as e:
            logger.exception(e)

            internal_server_error(e)

        return 'Request of live reindex all documents accepted', 202

    def update(self, uuid, index, scope):
        # Update a specific document with the passed in UUID
        # Takes in a document that will replace the existing one

        # Always expect a json body
        self.request_json_required(request)

        # Make sure the index is a composite index from configuration, and
        # the scope is valid for the composite index.
        self.validate_index(index) if index else None
        self.validate_scope(index, scope) if scope else None

        token = self.get_user_token(request.headers)
        document = request.json

        # Check if query parameter is passed to used futures instead of threading
        asynchronous = request.args.get('async')

        translator = self.init_translator(token)
        if asynchronous:
            try:
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(translator.update, uuid, document, index, scope)
                    result = future.result()
            except ValueError as ve:
                return str(ve), 400
            except Exception as e:
                logger.exception(e)
                internal_server_error(e)

            return result, 202

        else:
            try:
                threading.Thread(target=translator.update, args=[uuid, document, index, scope]).start()

                logger.info(f"Started to update document with uuid: {uuid}")
            except Exception as e:
                logger.exception(e)
                internal_server_error(e)

            return f"Request of updating {uuid} accepted", 202

    def add(self, uuid, index, scope):
        # Create a specific document with the passed in UUID
        # Takes in a document in the body of the request

        # Always expect a json body
        self.request_json_required(request)

        # Make sure the index is a composite index from configuration, and
        # the scope is valid for the composite index.
        self.validate_index(index) if index else None
        self.validate_scope(index, scope) if scope else None

        token = self.get_user_token(request.headers)
        document = request.json

        # Check if query parameter is passed to used futures instead of threading
        asynchronous = request.args.get('async')

        translator = self.init_translator(token)
        if asynchronous:
            try:
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(translator.add, uuid, document, index, scope)
                    result = future.result()
            except ValueError as ve:
                return str(ve), 400
            except Exception as e:
                logger.exception(e)
                internal_server_error(e)

            return result, 202

        else:
            try:
                threading.Thread(target=translator.add, args=[uuid, document, index, scope]).start()

                logger.info(f"Started to add document with uuid: {uuid}")
            except Exception as e:
                logger.exception(e)
                internal_server_error(e)

            return f"Request of adding {uuid} accepted", 202

    def clear_docs(self, index, scope, uuid):
        # Clear multiple documents from the specified composite index, in an
        # app-appropriate way expressed in a Translator delete_docs() method.
        # Either delete just the documents for the Dataset with the specified UUID,
        # or delete all the documents in the specified index

        # Make sure the index is a composite index from configuration, and
        # the scope is valid for the composite index.
        self.validate_index(index) if index else None
        self.validate_scope(index, scope) if scope else None

        if uuid:
            msgAck = (  f"Request to clear documents for uuid '{uuid}' from index '{index}'"
                        f", scope '{scope}',"
                        f" accepted")
            # Data Admin group not required to clear all documents for a Dataset from the ES index, but
            # will check write permission for the entity below.
            token = self.get_user_token(request.headers, admin_access_required=False)
        else:
            msgAck = f"Request to clear all documents from index '{index}', scope '{scope}' accepted"
            # The token needs to belong to the Data Admin group to be able to clear all documents in the ES index.
            token = self.get_user_token(request.headers, admin_access_required=True)

        if request.is_json:
            bad_request_error(f"An unexpected JSON body was attached to the request to clear documents from ES index '{index}', scope '{scope}'.")

        # Check if query parameter is passed to used futures instead of threading
        asynchronous = request.args.get('async')

        translator = self.init_translator(token)

        target_index_dict = {scope: 'TBD'} if scope else {'public': 'TBD', 'private': 'TBD'}
        if uuid:
            try:
                # If the uuid is for a Dataset, verify the user has write permission for the Dataset before
                # clearing any of the file info documents from the OpenSearch index
                self._verify_dataset_permission(dataset_uuid=uuid, token=token, translator=translator)
                logger.log(logging.DEBUG-1
                           ,f"Request received to delete file info documents in Dataset {uuid}.")
                target_index_dict[scope] = 'FOUND'
            except requests.HTTPError as heDataset:
                # If entity-api threw an exception trying to retrieve the entity for the uuid, it
                # is possible that it is the UUID of a File (which is not in Neo4j.) See if a
                # file info document can be retrieved from an OpenSearch index, so the Dataset permissions
                # can be checked.

                # Determine the target index in OpenSearch to be searched to check for the File's file info document.
                for composite_scope in target_index_dict.keys():
                    target_index = self.INDICES['indices'][index][composite_scope]
                    file_info_query_dict = {"query": {"match": {"file_uuid": uuid}}
                                            ,"_source": ["dataset_uuid"]}
                    configured_base_url = self.INDICES['indices'][index]['elasticsearch']['url'].strip('/')
                    target_url = f"{configured_base_url}/{target_index}/_search"
                    logger.debug(f"For uuid={uuid}, trying to retrieve a file info document using target_url={target_url}")

                    response = requests.get(url=target_url
                                            ,headers={'Content-Type': 'application/json'}
                                            ,json=file_info_query_dict)
                    if response.status_code == 200 and len(json.loads(response.text)['hits']['hits']) == 1:
                        target_index_dict[composite_scope] = 'FOUND'
                        dataset_uuid = json.loads(response.text)['hits']['hits'][0]['_source']['dataset_uuid']
                        try:
                            # If the uuid is for a Dataset, verify the user has write permission for the Dataset before
                            # clearing any of the file info documents from the OpenSearch index
                            self._verify_dataset_permission(dataset_uuid=dataset_uuid, token=token, translator=translator)
                            logger.log(logging.DEBUG - 1
                                       , f"Request received to delete file info document of File {uuid}'"
                                         f" in Dataset {dataset_uuid}.")
                        except requests.HTTPError as heDatasetOfFile:
                            msg = ( f"For File uuid '{uuid}' with Dataset uuid '{dataset_uuid}, unable to retrieve"
                                    f" Dataset from index {target_index} to verify permissions.")
                            logger.log( logging.DEBUG - 1
                                        ,msg)
                            target_index_dict[composite_scope] = msg
                    else:
                        msg = ( f"Unable to determine OpenSearch mapping field for matching, due"
                                f" to being unable to retrieve a Dataset or File entity for uuid '{uuid}'."
                                f" from index {target_index}")
                        logger.log(logging.DEBUG - 1
                                   , msg)
                        target_index_dict[composite_scope] = msg

                # After looking everywhere requested for a File or Dataset with the uuid, determine
                # if it is available anywhere before proceeding.
                uuid_found_in_index = False
                for composite_scope in target_index_dict.keys():
                    uuid_found_in_index = uuid_found_in_index or target_index_dict[composite_scope] == 'FOUND'
                if not uuid_found_in_index:
                    msg = (f"Unable to retrieve a Dataset or File entity for uuid '{uuid}' from"
                           f" from the indices [{', '.join(target_index_dict.keys())}].")
                    return msg, 404

        if asynchronous:
            try:
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(translator.delete_docs, index, scope, uuid)
                    result = future.result()
            except ValueError as ve:
                return str(ve), 400
            except Exception as e:
                logger.exception(e)
                internal_server_error(e)

            return result, 202

        else:
            try:
                threading.Thread(target=translator.delete_docs, args=[index, scope, uuid]).start()

                logger.info(f"Started to clear documents for uuid '{uuid}' from index '{index}', scope '{scope}'.")
            except Exception as e:
                logger.exception(e)
                internal_server_error(e)

            return msgAck, 202

    # Get user information dict based on the http request(headers)
    # `group_required` is a boolean, when True, 'hmgroupids' is in the output
    def get_user_info_for_access_check(self, request, group_required):
        return self.auth_helper_instance.getUserInfoUsingRequest(request, group_required)

    """
    Parse the token from Authorization header

    Parameters
    ----------
    request_headers: request.headers
        The http request headers
    admin_access_required : bool
        If the token is required to belong to the Data Admin group, default to False

    Returns
    -------
    str
        The token string if valid
    """

    def get_user_token(self, request_headers, admin_access_required=False):
        # Get user token from Authorization header
        # getAuthorizationTokens() also handles MAuthorization header but we are not using that here
        try:
            user_token = self.auth_helper_instance.getAuthorizationTokens(request_headers)
        except Exception:
            msg = "Failed to parse the Authorization token by calling commons.auth_helper.getAuthorizationTokens()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
            internal_server_error(msg)

        # The user_token is flask.Response on error
        if isinstance(user_token, Response):
            # The Response.data returns binary string, need to decode
            unauthorized_error(user_token.data.decode())

        if admin_access_required:
            # By now the token is already a valid token
            # But we also need to ensure the user belongs to Data Admin group
            # in order to execute the live reindex-all
            # Return a 403 response if the user doesn't belong to Data Admin group
            if not self.auth_helper_instance.has_data_admin_privs(user_token):
                forbidden_error("Access not granted")
        return user_token

    """
    Get user infomation dict based on the http request(headers)
    The result will be used by the trigger methods

    Parameters
    ----------
    request : Flask request object
        The Flask request passed from the API endpoint 

    Returns
    -------
    dict
        A dict containing all the user info

        {
            "scope": "urn:globus:auth:scope:nexus.api.globus.org:groups",
            "name": "First Last",
            "iss": "https://auth.globus.org",
            "client_id": "21f293b0-5fa5-4ee1-9e0e-3cf88bd70114",
            "active": True,
            "nbf": 1603761442,
            "token_type": "Bearer",
            "aud": ["nexus.api.globus.org", "21f293b0-5fa5-4ee1-9e0e-3cf88bd70114"],
            "iat": 1603761442,
            "dependent_tokens_cache_id": "af2d5979090a97536619e8fbad1ebd0afa875c880a0d8058cddf510fc288555c",
            "exp": 1603934242,
            "sub": "c0f8907a-ec78-48a7-9c85-7da995b05446",
            "email": "email@pitt.edu",
            "username": "username@pitt.edu",
            "hmscopes": ["urn:globus:auth:scope:nexus.api.globus.org:groups"],
        }
    """

    def get_user_info(self, request):
        # `group_required` is a boolean, when True, 'hmgroupids' is in the output
        user_info = self.auth_helper_instance.getUserInfoUsingRequest(request, True)

        logger.debug("======get_user_info()======")
        logger.debug(user_info)

        # It returns error response when:
        # - invalid header or token
        # - token is valid but not nexus token, can't find group info
        if isinstance(user_info, Response):
            # Bubble up the actual error message from commons
            # The Response.data returns binary string, need to decode
            msg = user_info.get_data().decode()
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
            raise Exception(msg)

        return user_info

    # Always expect a json body
    def request_json_required(self, request):
        if not request.is_json:
            bad_request_error("A JSON body and appropriate Content-Type header are required")

    # We'll need to verify the requested index in URL is valid
    def validate_index(self, index_without_prefix):
        separator = ','
        # indices = get_filtered_indices()
        indices = self.INDICES['indices'].keys()

        if index_without_prefix not in indices:
            bad_request_error(f"Invalid index name '{index_without_prefix}'. Use one of the following: {separator.join(indices)}")

    # Given a scope for a composite index from a user request, confirm there is
    # a known OpenSearch index configured for the composite index with that scope.
    def validate_scope(self, validated_index, scope):
        # N.B. use of self.INDICES['indices'][validated_index].keys() will allow bad values for
        # scope to pass validation only to fail when trying to find the correct OpenSearch index
        # name. Will also offer spurious names in the error message.
        # But not currently for public usage, so live with it until willing to modify config YAML.
        scopes = self.INDICES['indices'][validated_index].keys()
        if scope not in scopes:
            bad_request_error(  f"Invalid scope '{scope}' for index '{validated_index}'."
                                f" Use one of the following: {', '.join(scopes)}")

    # Determine the target real index in Elasticsearch bases on the request header and given index (without prefix)
    # The Authorization header with globus token is optional
    # Case #1: Authorization header is missing, default to use the `<project_prefix>_public_<index_without_prefix>`.
    # Case #2: Authorization header with valid token, but the member doesn't belong to the Globus Read group, direct the call to `<project_prefix>_public_<index_without_prefix>`.
    # Case #3: Authorization header presents but with invalid or expired token, return 401 (if someone is sending a token, they might be expecting more than public stuff).
    # Case #4: Authorization header presents with a valid token that has the read group access, direct the call to `<project_prefix>_consortium_<index_without_prefix>`.
    def get_target_index(self, request, index_without_prefix):
        # Case #1 and #2
        target_index = None

        # Keys in request.headers are case insensitive
        if 'Authorization' in request.headers:
            # user_info is a dict
            user_info = self.get_user_info_for_access_check(request, True)

            logger.info("======user_info======")
            logger.info(user_info)

            # Case #3
            if isinstance(user_info, Response):
                # Notify the client with 401 error message
                unauthorized_error(
                    "The globus token in the HTTP 'Authorization: Bearer <globus-token>' header is either invalid or expired.")
            # Otherwise, we check user_info['hmgroupids'] list
            # Key 'hmgroupids' presents only when group_required is True
            else:
                # Case #4
                token = self.get_user_token(request.headers)
                if self.auth_helper_instance.has_read_privs(token):
                    target_index = self.INDICES['indices'][index_without_prefix]['private']

        if target_index is None:
            return self.INDICES['indices'][index_without_prefix]['public']
        else:
            return target_index

    # Get a list of entity uuids via entity-api for a given entity type:
    # Collection, Donor, Sample, Dataset, Submission. Case-insensitive.
    def get_uuids_by_entity_type(self, entity_type, token):
        entity_type = entity_type.lower()

        request_headers = self.create_request_headers_for_auth(token)

        # Use different entity-api endpoint for Collection
        if entity_type == 'collection':
            url = self.DEFAULT_ENTITY_API_URL + "/collections?property=uuid"
        else:
            url = self.DEFAULT_ENTITY_API_URL + "/" + entity_type + "/entities?property=uuid"

        response = requests.get(url, headers=request_headers, verify=False)

        if response.status_code != 200:
            internal_server_error(
                "get_uuids_by_entity_type() failed to make a request to entity-api for entity type: " + entity_type)

        uuids_list = response.json()

        return uuids_list

    # Create a dict with HTTP Authorization header with Bearer token
    def create_request_headers_for_auth(self, token):
        auth_header_name = 'Authorization'
        auth_scheme = 'Bearer'

        headers_dict = {
            # Don't forget the space between scheme and the token value
            auth_header_name: auth_scheme + ' ' + token
        }

        return headers_dict

    def init_translator(self, token):
        return self.translator_module.Translator(self.INDICES, self.APP_CLIENT_ID, self.APP_CLIENT_SECRET, token)

    # Get a list of filtered Elasticsearch indices to expose to end users without the prefix
    def get_filtered_indices(self):
        # just get all the defined index keys from the yml file
        indices = self.INDICES['indices'].keys()
        return list(indices)


# For local development/testing
if __name__ == "__main__":
    try:
        app.run(host='0.0.0.0', port="5005")
    except Exception as e:
        print("Error during starting debug server.")
        print(str(e))
        logger.error(e, exc_info=True)
        print("Error during startup check the log file for further information")
