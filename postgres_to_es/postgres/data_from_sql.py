import psycopg2.extras
import psycopg2

from psycopg2.extensions import connection as _connection
from typing import Union
from postgres_to_es.postgres.data import ESData
from loguru import logger


class PostgresSaver:
    def __init__(self, pg_conn: _connection):

        self.conn = pg_conn
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    @logger.catch
    def extract_data_from_movies(self, data_class=ESData, date_start: str = '2000-06-16 20:14:09') -> ESData:

        def get_values(cursor: _connection.cursor) -> ESData:

            def processing_row(element: dict, name_element: str) -> Union[str, list, dict]:

                def process_actors_writers(current_element: Union[str, list, float]) -> Union[str, list, float]:
                    try:

                        actors_writers = [eval(el.replace('(', '("').replace(',', '",')) for el in
                                          current_element.replace("\\", '').replace('{"', '').replace('"}', '').split(
                                              '","')]

                        return [{"id": el[0], "name": el[1]} for el in actors_writers]

                    except AttributeError:
                        return [{'id': None, 'name': None}]

                    except (NameError, SyntaxError):
                        bad_list = current_element.replace("\\", '').replace('{"', '').replace('"}', '').split('","')
                        bad_index = [idx for idx, el in enumerate(bad_list) if el.count('"') == 0]
                        for idx in bad_index:
                            bad_list[idx] = bad_list[idx].replace(",", ',"').replace(')', '")')
                        bad_actors_writers = [eval(el.replace('(', '("').replace(',', '",')) for el in bad_list]
                        return [{"id": el[0], "name": el[1]} for el in bad_actors_writers]

                def process_writers_names_actors_names_director_genres(current_element: Union[str, list, float]) -> \
                        Union[list[str], str]:
                    try:
                        result = ' '.join(current_element)
                        if result != ' ':
                            return [result]
                        else:
                            return 'null'
                    except TypeError:
                        return 'null'

                process_funcs = {
                    'actors_writers': process_actors_writers,
                    'writers_names_actors_names_director_genre': process_writers_names_actors_names_director_genres
                }
                if name_element in ['actors', 'writers']:
                    return process_funcs['actors_writers'](element[name_element])
                elif name_element in ['genre', 'director', 'actors_names', 'writers_names']:
                    return process_funcs['writers_names_actors_names_director_genre'](element[name_element])

                return element[name_element]

            while (current_value := cursor.fetchone()) is not None:
                dict_current_value = dict(current_value)

                for field in dict_current_value:
                    dict_current_value[field] = processing_row(element=dict_current_value, name_element=field)

                    if dict_current_value[field] is None:
                        if dict_current_value['imdb_rating'] is None:
                            dict_current_value['imdb_rating'] = 0
                        else:
                            dict_current_value[field] = ""

                yield data_class(**dict_current_value)

        sql_request = \
            f"""
                 
            WITH all_perosn AS (
                                SELECT id
                                FROM movies_test.content.person
                                WHERE updated_at > '{date_start}'
                                ORDER BY updated_at
                                ),
                                 all_films AS(
                                    SELECT fw.id AS id
                                    FROM movies_test.content.film_work fw
                                    LEFT JOIN movies_test.content.person_film_work pfw ON pfw.film_work_id = fw.id
                                    WHERE pfw.person_id IN (SELECT id FROM all_perosn)
                                    ORDER BY fw.updated_at
                                 )
        
            SELECT
                    fw.id as id,
                    fw.rating as imdb_rating,
                    ARRAY_AGG(DISTINCT g.name ) AS genre,
                    fw.title as title,
                    fw.description as description,
                    ARRAY_AGG(DISTINCT p.full_name )
                        FILTER ( WHERE pfw.role = 'director' ) AS director,
                    ARRAY_AGG(DISTINCT p.full_name)
                        FILTER ( WHERE pfw.role = 'actor' ) AS actors_names,
                    ARRAY_AGG(DISTINCT p.full_name)
                        FILTER ( WHERE pfw.role = 'writer' ) AS writers_names,
                    ARRAY_AGG(DISTINCT (p.id, p.full_name))
                        FILTER ( WHERE pfw.role = 'actor' ) AS actors,
                    ARRAY_AGG(DISTINCT (p.id, p.full_name))
                        FILTER ( WHERE pfw.role = 'writer' ) AS writers
    
                FROM movies_test.content.film_work fw
                LEFT JOIN movies_test.content.person_film_work pfw ON pfw.film_work_id = fw.id
                LEFT JOIN movies_test.content.person p ON p.id = pfw.person_id
                LEFT JOIN movies_test.content.genre_film_work gfw ON gfw.film_work_id = fw.id
                LEFT JOIN movies_test.content.genre g ON g.id = gfw.genre_id
                WHERE fw.id IN (SELECT id FROM all_films)
                GROUP BY fw.id;
            """
        self.cursor.execute(sql_request)

        logger.info('SQL запрос выполнен')
        return get_values(self.cursor)
