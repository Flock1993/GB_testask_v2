import os
import time

from apscheduler.schedulers.background import BackgroundScheduler
from pytz import utc

from parsing import main

if __name__ == "__main__":

    # по ТЗ
    # запуск импорта телеметрии 1 раз в 1 час
    # для отладки
    # запуск импорта телеметрии 1 раз в 6 секунд

    scheduler = BackgroundScheduler(timezone=utc)
    scheduler.add_job(func=main, trigger='interval', seconds=5)
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
    try:
        scheduler.start()
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        pass
