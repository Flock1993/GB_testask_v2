from parsing import db_connection, JsonPars

from fastapi import FastAPI, status, HTTPException
from pydantic import BaseModel
from datetime import datetime


with db_connection() as db_conn:
    app = FastAPI()

instanse = JsonPars()
REQUIRED_SENSORS = instanse.create_collections()[0]


# запуск приложения uvicorn main:app --reload
# эндпоинт swagger http://127.0.0.1:8000/docs
class POSTItem(BaseModel):
    timestamp: str
    sensor_values: list[dict]
    '''
    Пример payload
    {
    "timestamp": "2021-07-15 10:36:54",
    "sensor_values": [
        {"sensor_id": "sensor1", "value": 1.074},
        {"sensor_id": "sensor2", "value": 54.295}
    ]
    }
    '''


def insert_in_db(cnxn, timestamp, values_to_write):
    """Запись данных из тела POST запроса в БД"""
    with cnxn:
        cursor = cnxn.cursor()
        cursor.execute(f"""
            INSERT INTO sensor_value(ts, {', '.join(REQUIRED_SENSORS)})
            VALUES('{timestamp}', {', '.join(values_to_write)})
            ON CONFLICT DO NOTHING;
        """)
        cursor.execute("""
            SELECT *
            FROM sensor_value;
        """)
        print(cursor.fetchall())


def select_last_value(cnxn, sensor_id):
    """Выбрать последнее значение по указанному датчику из БД"""
    try:
        with cnxn:
            cursor = cnxn.cursor()
            cursor.execute(f"""
                SELECT ts, {sensor_id}
                FROM sensor_value
                WHERE ts = (
                    SELECT MAX(ts)
                    FROM sensor_value);
            """)
            lst = cursor.fetchall()
            timestamp = lst[0][0]
            value = lst[0][1]
        return timestamp, value
    except Exception as err:
        return None, err, type(err)


def max_timestamp(cnxn):
    """Определение максимального timestamp в БД"""
    with cnxn:
        cursor = cnxn.cursor()
        cursor.execute("""
            SELECT MAX(ts)
            FROM sensor_value
        """)
    return cursor.fetchone()[0]


def valid_data(timestamp, sensor_values, max_timestamp):
    """Валидация данных из тела POST запроса"""
    if max_timestamp is not None:
        if timestamp < max_timestamp:
            print(f'timestamp запроса {timestamp} меньше чем MAX timestamp БД {max_timestamp}')
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f'timestamp запроса {timestamp} меньше чем MAX timestamp БД {max_timestamp}')
    set_db = set(REQUIRED_SENSORS)
    sensor_names_post = set([x['sensor_id'] for x in sensor_values])
    absent_sensors = sensor_names_post.difference(set_db)
    exist_sensors = set_db.intersection(sensor_names_post)
    di = {item["sensor_id"]: item["value"] for item in sensor_values}
    values_to_write = [str(di.get(sensor_name, 'NULL')) for sensor_name in REQUIRED_SENSORS]
    print(values_to_write)
    return absent_sensors, exist_sensors, values_to_write  # named turple


@app.post(
    "/api/write_values",
)
def post_root(item: POSTItem):
    timestamp = datetime.strptime(item.timestamp, '%Y-%m-%d %H:%M:%S')
    sensor_values = item.sensor_values
    maxi = max_timestamp(db_conn)
    data = {
        "status": "Success",
        "desc": "Данные датчиков были успешно записаны в БД"
    }
    res = valid_data(timestamp, sensor_values, maxi)
    absent_sensors, exist_sensors, values_to_write = res
    insert_in_db(db_conn, timestamp, values_to_write)
    if len(absent_sensors) != 0:
        data["desc"] = f"Показания следующих датчиков {absent_sensors} не были импортированы"
    return status.HTTP_200_OK, data


@app.get("/api/last_value")
def read_root(sensor_id: str):
    my_tuple = select_last_value(db_conn, sensor_id)
    if len(my_tuple) == 2:
        timestamp, last_value = my_tuple
        data = {
            "timestamp": timestamp,
            f"{sensor_id}": last_value
        }
        return status.HTTP_200_OK, data
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f'"desc": "Ошибка {my_tuple[1]}, тип ошибки {my_tuple[2]}"')
