import logging

from libs.es_writer import ESWriter

logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


class Indexer:
    def index(self, entity_id, document, index_name, reindex=False):
        # Delete old doc for reindex
        if reindex:
            logger.debug(f"Deleting old document with uuid: {entity_id} from index: {index_name}")
            self.delete_document(entity_id, index_name)

        logger.debug(f"Creating document with uuid: {entity_id} at index: {index_name}")
        self.eswriter.write_or_update_document(index_name=index_name, doc=document, uuid=entity_id)

    def create_index(self, index_name, index_settings):
        self.eswriter.create_index(index_name, index_settings)

    def delete_document(self, entity_id, index_name):
        self.eswriter.delete_document(index_name, entity_id)

    def delete_index(self, index_name):
        self.eswriter.delete_index(index_name)

    def __init__(self, indices, default_index):
        self.elasticsearch_url = indices[default_index]['elasticsearch']['url'].strip('/')

        self.eswriter = ESWriter(self.elasticsearch_url)
