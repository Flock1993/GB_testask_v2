from datetime import datetime

from fastapi import FastAPI, status, HTTPException
from pydantic import BaseModel

from parsing import db_connection, JsonPars

with db_connection() as db_conn:
    app = FastAPI()

instanse = JsonPars()
REQUIRED_SENSORS = instanse.create_collections()[0]


class POSTItem(BaseModel):
    """
    Пример payload
    {
    "timestamp": "2021-07-15 10:36:54",
    "sensor_values": [
        {"sensor_id": "sensor1", "value": 1.074},
        {"sensor_id": "sensor2", "value": 54.295}
    ]
    }
    """
    timestamp: str
    sensor_values: list[dict[str, float]]


def valid_data(timestamp: datetime, max_timestamp: datetime, sensor_values: list[dict[str, float]]) -> (
        set[str], list[str]):
    """Валидация данных из тела POST запроса"""
    if timestamp < max_timestamp:
        return 'timestamp меньше чем максимальный timestamp в БД'
    set_db = set(REQUIRED_SENSORS)
    sensor_names_post = set([x['sensor_id'] for x in sensor_values])
    absent_sensors = sensor_names_post.difference(set_db)
    di = {item["sensor_id"]: item["value"] for item in sensor_values}
    values_to_write = [str(di.get(sensor_name, 'NULL')) for sensor_name in REQUIRED_SENSORS]
    return absent_sensors, values_to_write


def max_timestamp(cnxn):
    """Определение максимального timestamp в БД"""
    with cnxn:
        cursor = cnxn.cursor()
        cursor.execute("""
            SELECT MAX(ts)
            FROM sensor_value
        """)
    return cursor.fetchone()[0]


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
        print(type(cnxn))
        print(cursor.fetchall())


@app.post("/api/write_values")
def post_root(item: POSTItem):
    try:
        timestamp = datetime.strptime(item.timestamp, '%Y-%m-%d %H:%M:%S')
        sensor_values = item.sensor_values
        maxi = max_timestamp(db_conn)
        data = {
            "status": "Success",
            "desc": "Данные датчиков были успешно записаны в БД"
        }
        absent_sensors, values_to_write = valid_data(timestamp, maxi, sensor_values)
        insert_in_db(db_conn, timestamp, values_to_write)
        if len(absent_sensors) != 0:
            data["desc"] = f"Показания следующих датчиков {absent_sensors} не были импортированы"
        return status.HTTP_200_OK, data
    except Exception as err:
        data = {
            "status": "Error",
            "desc": type(err)
        }
        return status.HTTP_400_BAD_REQUEST, data


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


# @app.get("/api/last_value")
# def read_root(response: Response, sensor_id: str):
#     my_tuple = select_last_value(db_conn, sensor_id)
#     if len(my_tuple) == 2:
#         timestamp, last_value = my_tuple
#         data = {
#             "timestamp": timestamp,
#             f"{sensor_id}": last_value
#         }
#         return status.HTTP_200_OK, data
#     else:
#         response.status_code = status.HTTP_400_BAD_REQUEST
#         data = {
#             "status": "Error",
#             "desc": my_tuple[2]
#         }
#         return status.HTTP_400_BAD_REQUEST, data

