import os
import psycopg2
import backoff
import psycopg2.errors
import datetime

from pprint import pp
from typing import Union, Any
from loguru import logger
from postgres_to_es.postgres.data_from_sql import PostgresSaver
from postgres_to_es.es.es import ElasticSearch
from postgres_to_es.es.es_state import JsonFileStorage, State
from postgres_to_es.config import FILE_PATH_INDEX_BODY, FILE_PATH_STATE, INDEX_NAME, dsl


class ETL:
    def __init__(self, options: dict = dsl):
        self.new_records_id = []
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

        update_date = postgres.extract_data_from_movies(date_start=data_start)
        logger.info("Обновленные данные загружены")
        if not update_date:
            logger.warning('Обновлений нет, получен пустой словарь')

        return {el.__dict__['id']: el.__dict__ for el in update_date}

    @logger.catch
    def transform(self, state: dict) -> None:
        storage = State(storage=self.json_file_storage)
        local_state = storage.state
        if local_state is None:
            self.json_file_storage.save_state(state=state)
            logger.info(f"Все данные состояния загружены в файл состояния {FILE_PATH_STATE}")
        else:
            storage_ids = tuple(local_state.keys())
            if not state:
                logger.warning('Нет новых данных для записи в Файл состояния')
            else:
                logger.info('Есть новые данные')
                for id_current in state:
                    self.new_records_id.append(id_current)
                    if id_current in storage_ids:
                        logger.warning(f"Обновляем значение в файле состояния, id={id_current}")
                        storage.set_state(key=id_current, value=state[id_current])
                        logger.info(f"Обновлено значение в файле состояния, id={id_current}")
                    else:
                        logger.warning(f"Добавляем значение в файл состояния, id={id_current}")
                        storage.set_state(key=id_current, value=state[id_current])
                        logger.info(f"Добавлено значение в файл состояния, id={id_current}")

    @logger.catch
    def load(self) -> Union[Any, None]:
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

            raws_es = self.__transform_data_for_es(state_data=state_data, records_id=self.new_records_id)
            if raws_es:
                self.elastic.insert_values_to_es(data_inserts=raws_es)
        else:
            return logger.warning("Отсутсвует файл состояния")

    @staticmethod
    def __transform_data_for_es(state_data: dict, records_id: list) -> Union[list, None]:
        if records_id:
            raws_es = []
            for id_raw in state_data:
                if id_raw in records_id:
                    record = {
                        "_index": INDEX_NAME,
                        "_id": id_raw,
                        "_source": state_data[id_raw]
                    }
                    raws_es.append(record)
            return raws_es

        logger.warning('Нет новых записей для ES')
        return None


def modification_date(filename: str) -> str:
    t = os.path.getmtime(filename) - 3 * 60 * 60
    return str(datetime.datetime.fromtimestamp(t))
