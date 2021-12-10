import os
from loguru import logger

from config import FILE_PATH_STATE, SHED_INTERVAL
from ETL.etl import ETL, modification_date
from apscheduler.schedulers.background import BackgroundScheduler


@logger.catch
def main():

    etl = ETL()

    if os.path.exists(FILE_PATH_STATE):
        last_change_time = modification_date(FILE_PATH_STATE)
        current_state = etl.extract(data_start=last_change_time)
    else:
        current_state = etl.extract()

    etl.transform(state=current_state)
    etl.load()


if __name__ == '__main__':
    logger.add('logs/logs.log', level='DEBUG', retention='1 days')

    scheduler = BackgroundScheduler(timezone="Europe/Moscow")
    scheduler.start()
    try:
        scheduler.add_job(main, 'interval', seconds=SHED_INTERVAL, id='ETL')
    except KeyboardInterrupt:
        scheduler.shutdown()



