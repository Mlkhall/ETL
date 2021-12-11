# Проектное задание: ETL

Написать отказоустойчивый перенос данных из Postgres в Elasticsearch

### Реализация состояния
- Файл состояния распологаестя в ```postgres_to_es/es/es_data/state``` и хранится в переменной FILE_PATH_STATE
- Ниже приведен блок отвечающий за реализацию мониторинга состояния:


```python
etl = ETL() 
# Если файл состояния существует в FILE_PATH_STATE
if os.path.exists(FILE_PATH_STATE):
    # Тогда мы забираем время последней модификации данного файла
    last_change_time = modification_date(FILE_PATH_STATE)
    # Отправляем это время в запрос в БД для получения обновленных 
    # записей
    current_state = etl.extract(data_start=last_change_time)
else:
    # Если файл не будет сущестовать, тогда заберем все записи
    # из БД
    current_state = etl.extract()
```
Мы всегда будет получать данные последнего редактирования файла, пока он 
существует

### Реализация backoff

За реализацию ```backoff``` отвечает модуль `backoff 1.11.1`:

https://pypi.org/project/backoff/

Происходит переподключение к БД при возникновении исключения
`psycopg2.OperationalError`
```python 
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
```

В теории есть предложение для реализации декоратора,
но есть несколько недостатков, ии-за чего я решил не прибегать к этой реализации:
1. Можно использовать готовое решение из коробки и не изобретать велосипед
2. В теории очень скудный материал для реализации декоратора, поэтому придется писать в slack и тратить время, на то чтоб разобраться в том, что уже реализовано
3. Дедлайн завтра, и нет возможности копаться в том, что предлагают в теории :(