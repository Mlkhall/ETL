import os
import json

from typing import Union, Any, Optional
from elasticsearch import Elasticsearch, helpers
from loguru import logger
from postgres_to_es.config import FILE_PATH_INDEX_BODY


class ElasticSearch:
    def __init__(self, host: str = 'localhost', port: int = 9200):
        self.es = Elasticsearch([{'host': host, 'port': port}])
        self.status = True if self.es.ping() else False
        if self.status:
            logger.info('Подключение выполнено')
        else:
            logger.warning('Нет подключения в ES')

    def create_index(self, index_name: str, body: dict, delete_exist_index: bool = True) -> Union[
        dict[str, Any], str, None]:
        if self.status:
            if not self.es.indices.exists(index=index_name):
                return self.es.indices.create(index=index_name, body=body)
            else:
                if delete_exist_index:
                    self.delete_index_by_name(index_name=index_name)
                    return logger.warning(f"Index already exists, then {index_name} was deleted")
                else:
                    return logger.warning("Index already exists")
        else:
            return logger.exception("Bad status")

    def get_all_indexes_names(self) -> list:
        logger.info('Получаем все индексы')
        return list(self.es.indices.get_alias().keys())

    def delete_index_by_name(self, index_name: str) -> dict[str, Any]:
        logger.warning(f'Удаляем индекс {index_name}')
        return self.es.indices.delete(index=index_name, ignore=[400, 404])

    def insert_values_to_es(self, data_inserts: list) -> None:
        logger.warning(f'Добавляем {len(data_inserts)} значений в ES')
        helpers.bulk(self.es, data_inserts)
        logger.info(f'Добавлны значения в ES')

    @staticmethod
    def get_body_from_json_file(file_path: str = FILE_PATH_INDEX_BODY) -> Optional[Any]:
        if os.path.exists(file_path):
            with open(file_path) as json_file:
                return json.load(json_file)
        else:
            return logger.exception("File not found")
