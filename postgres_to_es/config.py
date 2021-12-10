import os
from dotenv import load_dotenv


FILE_PATH_INDEX_BODY = './es/es_data/main_index_body.json'
FILE_PATH_STATE = './es/es_data/state/state.json'
INDEX_NAME = 'movies'

SHED_INTERVAL = 5

if load_dotenv('.env'):
    dsl = {'dbname': os.environ.get('DB_NAME', 'some_db'),
           'user': os.environ.get('DB_USER', 'defult_user'),
           'password': os.environ.get('PASSWORD'),
           'host': os.environ.get('HOST', '127.0.0.1'),
           'port': os.environ.get('PORT', 5432)}