from json_import.parsing import db_connection

from fastapi import FastAPI
from pydantic import BaseModel
from urllib import response
import datetime
import os
import psycopg2

# Не факт, что сработает. Если не получится, то оставить как есть
with db_conn() as db_conn:
    app = FastAPI()


class POSTItem(BaseModel):
    timestamp: str
    values: dict

    '''
    “Timestamp”:”2021-07-14 09:36:54.038”
    “sensor_values”: [
        {“sensor_id”:”sensor1”, “value”:1.074},
        {“sensor_id”:”sensor2”, “value”:54.295}
    ]
    '''

def valid_data():
    """валидация данных из тела POST запроса"""
    pass

def insert_in_db():
    """запись данных из тела POST запроса в БД"""
    pass

def select_last_value():
    """Выбрать последнее значение по указанному датчику из БД"""
    pass



@app.get("/last_value") #http://localhost:800/api/sensor_id=ensor_1
def root():
    # как выловить параметр их GET запроса? вернуть списком или словарём
    pass


@app.post("/write_values")
def root(POSTItem):
    try^
        ts = datetime.strptime(POSTItem.timestamp, 'YYYY')
        write_in_db(ts, POSTItem.values)
        cursor = db_conn.cursor()
        cursor.execute()
        for key in POSTItem.keys():
            write_in_db(ts, key, POSTItem.value[key])
        return (status succses, )
    except exeptiom as e:
        # поменять респонсе статус
        return(status: error, decs: str)

