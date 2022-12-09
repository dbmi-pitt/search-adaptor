import requests
import json
import os
import logging
import sys

# Set logging fromat and level (default is warning)
# All the API logging is forwarded to the uWSGI server and gets written into the log file `uwsgo-entity-api.log`
# Log rotation is handled via logrotate on the host system with a configuration file
# Do NOT handle log file and rotation via the Python logging to avoid issues with multi-worker processes
logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s:%(lineno)d: %(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

class ESWriter:
    def __init__(self, elasticsearch_url):
        self.elasticsearch_url = elasticsearch_url

    def write_document(self, index_name, doc, uuid):
        try:
            headers = {'Content-Type': 'application/json'}
            rspn = requests.post(f"{self.elasticsearch_url}/{index_name}/_doc/{uuid}", headers=headers, data=doc)
            if rspn.ok:
                logger.info(f"Added doc of uuid: {uuid} to index: {index_name}")
            else:
                logger.error(f"Failed to write {uuid} to elasticsearch, index: {index_name}")
                logger.error(f"Error Message: {rspn.text}")
                logger.info("==============ESWriter.write_document(): request body of JSON source==============")
                logger.info(doc)
        except Exception:
            msg = "Exception encountered during executing ESWriter.write_document()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    # This method uses the "Lucene query string syntax" to run a "query parameter search."
    # Per the links below, "Query parameter searches do not support the full Elasticsearch Query DSL but
    # are handy for testing."
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete-by-query.html
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html
    #
    # delete_document(self, index_name, uuid) could become a facade for
    # delete_fieldmatch_document(self, index_name, "uuid", uuid) to transition to Elasticsearch's
    # Query DSL JSON style.
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html
    def delete_document(self, index_name, uuid):
        try:
            headers = {'Content-Type': 'application/json'}
            rspn = requests.post(f"{self.elasticsearch_url}/{index_name}/_delete_by_query?q=uuid:{uuid}", headers=headers)
            if rspn.ok:
                logger.info(f"Deleted doc of uuid: {uuid} from index: {index_name}")
            else:
                logger.error(f"Failed to delete doc of uuid: {uuid} from index: {index_name}")
                logger.error(f"Error Message: {rspn.text}")
        except Exception:
            msg = "Exception encountered during executing ESWriter.delete_document()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def delete_fieldmatch_document(self, index_name, field_name, field_value):
        try:
            if not index_name:
                raise Exception(f"ESWriter.delete_fieldmatch_document() unable to execute with index_name '{index_name}.")
            post_url = f"{self.elasticsearch_url}/{index_name}/_delete_by_query?conflicts=proceed"

            if field_name and field_value:
                jsonQuery = f'{{"query": {{"match": {{"{field_name}": "{field_value}"}} }} }}'
                msgSuccess = f"Posted delete request for docs with field_name='{field_name}', field_value='{field_value}' from index '{index_name}'"
                msgFailure = f"Failed to post request to delete doc with field_name='{field_name}', field_value='{field_value}' from index '{index_name}'"
            else:
                jsonQuery = f'{{ "query": {{ "match_all": {{}} }} }}'
                msgSuccess = f"Posted request to delete all docs from index '{index_name}'"
                msgFailure = f"Failed to post request to delete all docs from index '{index_name}'"
            headers = {'Content-Type': 'application/json'}
            rspn = requests.post(post_url, headers=headers, data=jsonQuery)
            if rspn.ok:
                logger.info(msgSuccess)
            else:
                logger.error(msgFailure)
                logger.error(f"Error Message: {rspn.text}")
        except Exception as e:
            msgUnexpected = "Exception encountered during executing ESWriter.delete_fieldmatch_document() with field_name='{field_name}', field_value='{field_value}', index_name='{index_name}'"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msgUnexpected)
    def write_or_update_document(self, index_name='index', type_='_doc', doc='', uuid=''):
        try:
            headers = {'Content-Type': 'application/json'}
            rspn = requests.put(f"{self.elasticsearch_url}/{index_name}/{type_}/{uuid}", headers=headers, data=doc)
            if rspn.status_code in [200, 201, 202]:
                logger.info(f"Added doc of uuid: {uuid} to index: {index_name}")
                return f"Added doc of uuid: {uuid} to index: {index_name}"
            else:
                logger.error(f"Failed to write doc of uuid: {uuid} to index: {index_name}")
                logger.error(f"Error Message: {rspn.text}")
                logger.info("==============ESWriter.write_or_update_document(): request body of JSON source==============")
                logger.info(doc)
                return f"Failed to write doc of uuid: {uuid} to index: {index_name}. {rspn.text}"
        except Exception:
            msg = "Exception encountered during executing ESWriter.write_or_update_document()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
            return msg

    def delete_index(self, index_name):
        try:
            rspn = requests.delete(f"{self.elasticsearch_url}/{index_name}")

            if rspn.ok:
                logger.info(f"Deleted index: {index_name}")
            else:
                logger.error(f"Failed to delete index: {index_name} in elasticsearch.")
                logger.error(f"Error Message: {rspn.text}")
        except Exception:
            msg = "Exception encountered during executing ESWriter.delete_index()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    # The settings and mappings definition needs to be passed in via config
    def create_index(self, index_name, config):
        try:
            headers = {'Content-Type': 'application/json'}

            rspn = requests.put(f"{self.elasticsearch_url}/{index_name}", headers=headers, data=json.dumps(config))
            if rspn.ok:
                logger.info(f"Created index: {index_name}")
            else:
                logger.error(f"Failed to create index: {index_name} in elasticsearch.")
                logger.error(f"Error Message: {rspn.text}")
        except Exception:
            msg = "Exception encountered during executing ESWriter.create_index()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
