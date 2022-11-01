import json
import logging
import os
from datetime import datetime, timedelta
from decimal import *
from typing import Any, Dict, Optional, Tuple
import psycopg2
from dotenv import load_dotenv


load_dotenv()

server = 'localhost'
db = os.getenv('DB_NAME', default='DB')
username = os.getenv('POSTGRES_USER')
psswrd = os.getenv('POSTGRES_PASSWORD')

cnxn = psycopg2.connect(
    host=server,
    database=db,
    user=username,
    password=psswrd,
)


def parsing_datetime(file_name: str) -> Optional[datetime]:
    """Парсинг даты и времени в названии файла"""
    strt: int = file_name.find('_')
    nd: int = file_name.find('_', strt + 1)
    year, month, day = file_name[strt + 1:nd].split('-')

    strt = nd
    nd = file_name.find('.')
    hours, minutes, seconds = file_name[strt + 1:nd].split('_')

    try:
        return datetime(int(year), int(month), int(day),
                        int(hours), int(minutes), int(seconds))
    except ValueError:
        return None


def delta_datetime(str_data: str, pars_datetime: datetime
                   ) -> Optional[timedelta]:
    """
    Отладочная функция расчета разницы datatime имени файла и содержимого
    Дата в названии файла и его содержимом отличается
    """
    try:
        content_data = datetime.strptime(str_data, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        print('Некорректная строка даты')
    if pars_datetime != content_data:
        return pars_datetime - content_data
    return None


def process_file(json_file: str) -> bool:
    """Проверка названия и формата файла на корректность"""
    # Длина названия файла соотвествует корректному имени в папке telemetry
    if json_file.startswith('sensors_') and (
            json_file.endswith('.json')) and len(json_file) == 32:
        return True
    return False


class JsonPars:
    """Передача показаний датчиков"""

    DIR_TELEM = 'data/telemetry'
    DIR_CONFIG = '../data'
    REF_POINT = datetime(2021, 7, 15, 10, 0, 0)

    # для "боевого" применения класса использовать datetime.now()

    def __init__(self,
                 dir_config: str = DIR_CONFIG,
                 dir_telemetry: str = DIR_TELEM,
                 conf_datetime: datetime = REF_POINT,
                 ) -> None:
        self.dir_telemetry = dir_telemetry
        self.dir_config = dir_config
        self.conf_datetime = conf_datetime

    def create_collections(
            self) -> Tuple[Any, Dict[Any, Any], Dict[Any, Dict[str, int]]]:
        """
        Внесение названий целевых датчиков во вспомогательные коллекции
        и их запись в БД
        """
        with open(f'{self.dir_config}/config.json',
                  encoding='utf-8') as target_sensors:

            lst_sensors = json.load(target_sensors)['loading_sensors']
            lst_1 = [((num, name) for num, name in enumerate(lst_sensors))]
            with cnxn:
                cursor = cnxn.cursor()
                cursor.executemany(
                    'INSERT INTO sensor(id, sensor_name)'
                    " VALUES (?, '?');", lst_1
                )

            dict_sensors = {x: x for x in lst_sensors}
            mid_result = {x: {'count': 0, 'summ': 0} for x in
                          lst_sensors}
        return lst_sensors, dict_sensors, mid_result

    def calc_write_db(self, mid_result: Dict[Any, Dict[str, int]]) -> Tuple[
            Dict[str, int], Any]:
        """Вычисление средних показателей и запись в базу данных"""
        result = {}
        for sensor, sensor_values in mid_result.items():
            if sensor_values['count'] == 0:
                result[sensor] = 0
            else:
                result[sensor] = Decimal(
                    sensor_values['summ'] / sensor_values['count']).quantize(
                    Decimal('1.000'))
        #count_0 = SensorValue.objects.count()
        #for sens, value in result.items():
        #    SensorValue.objects.get_or_create(
        #        sensor=Sensor.objects.get(sensor_id=sens),
        #        sensor_value=value,
        #        timestamp=self.conf_datetime
        #    )
       # count = SensorValue.objects.count()
        #delta_obj = count - count_0
        #return result, delta_obj

    def process_telemetry(self) -> bool:
        """Главная функция передачи показаний датчиков в БД"""
        lst_sensors, dict_sensors, mid_result = self.create_collections()
        for json_file in os.listdir(self.dir_telemetry):
            if process_file(json_file):
                proces_datetime = parsing_datetime(json_file)
                if proces_datetime is not None:
                    if self.conf_datetime <= proces_datetime < (
                            self.conf_datetime + timedelta(hours=1)):
                        with open(f'{self.dir_telemetry}/{json_file}',
                                  encoding='utf-8') as pars_file:
                            sensors_data = json.load(pars_file)
                            for sensor in sensors_data['sensors']:
                                sens_id = sensor['sensor_id']
                                value = sensor['value']
                                if dict_sensors.get(sens_id) is None:
                                    continue
                                mid_result[sens_id]['count'] += 1
                                mid_result[sens_id]['summ'] += value
                else:
                    logging.info(
                        f'Дата в названии файла {json_file} некорректна')
            else:
                logging.info(
                    f'Название или формат файла {json_file} некорректны')
        result, delta_obj = self.calc_write_db(mid_result)
        if delta_obj > 0:
            logging.info(
                f'Импорт показаний датчиков выполнен успешно {result}')
            return True
        logging.info('Количество записей в модели SensorValue не изменилось')
        return False


if __name__ == "__main__":
    instanse = JsonPars()
    instanse.create_collections()
