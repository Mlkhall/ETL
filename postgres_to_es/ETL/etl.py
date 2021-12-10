import os
import psycopg2
import backoff
import psycopg2.errors
import datetime

from typing import Union
from loguru import logger
from postgres_to_es.postgres.data_from_sql import PostgresSaver
from postgres_to_es.es.es import ElasticSearch
from postgres_to_es.es.es_state import JsonFileStorage, State
from postgres_to_es.config import FILE_PATH_INDEX_BODY, FILE_PATH_STATE, INDEX_NAME, dsl


class ETL:
    def __init__(self, options: dict = dsl):
        self.options = options
        self.elastic = ElasticSearch()
        self.json_file_storage = JsonFileStorage(file_path=FILE_PATH_STATE)

    @logger.catch
    @backoff.on_exception(backoff.expo, exception=psycopg2.OperationalError)
    def extract(self, data_start: Union[str, None] = None) -> dict:

        logger.info('Выпоняется подключение к БД')
        with psycopg2.connect(**self.options) as pg_conn:
            postgres = PostgresSaver(pg_conn=pg_conn)
            logger.info('Подключение выполнено')

        if data_start is None:
            logger.warning("Нет времени последнего изменения, считываем все данные")
            return {el.__dict__['id']: el.__dict__ for el in postgres.extract_data_from_movies()}

        else:
            update_date = postgres.extract_data_from_movies(date_start=data_start)
            logger.info("Обновленные данные загружены")
            if not update_date:
                logger.warning('Обновлений нет, получен пустой словарь')
            return {el.__dict__['id']: el.__dict__ for el in update_date}

    @logger.catch
    def transform(self, state: dict) -> None:
        self.json_file_storage.save_state(state=state)
        logger.info("Данные состояния загружены")

    @logger.catch
    def load(self):
        all_indexes = self.elastic.get_all_indexes_names()
        if INDEX_NAME not in all_indexes:
            if os.path.exists(FILE_PATH_INDEX_BODY):
                body_index = self.elastic.get_body_from_json_file(file_path=FILE_PATH_INDEX_BODY)
                self.elastic.create_index(index_name=INDEX_NAME, body=body_index)

                logger.warning(f"Нет индекса {INDEX_NAME}, создан индекс")
            else:
                return logger.warning("Отсутствует файл со схемой индекса")
        if os.path.exists(FILE_PATH_STATE):
            state = State(storage=self.json_file_storage)
            state_data = state.state
            for id_raw in state_data:
                row = state_data[id_raw]
                self.elastic.insert_values_to_es(index_name=INDEX_NAME, id_=id_raw, body=row)
                logger.info(f"Записано значение в EL для {id_raw}")
        else:
            return logger.warning("Отсутсвует файл состояния")


def modification_date(filename: str) -> str:
    t = os.path.getmtime(filename) - 3 * 60 * 60
    return str(datetime.datetime.fromtimestamp(t))
