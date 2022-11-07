from datetime import datetime

import requests

from ..parsing import db_connection

URL = "http://127.0.0.1:8000/api"


def query_execute(sql_query):
    """Выполнение SQL запроса для тестовой проверки"""
    with db_connection() as db_con:
        cursor = db_con.cursor()
        cursor.execute(sql_query)
        entry_exist = cursor.fetchone()[0]
    return entry_exist


def test_get_smoke(clear_db, telemetry_fixture, script_execute):
    """Корректная работа метода GET"""
    payload = {"sensor_id": "sensor1"}
    response = requests.get(f"{URL}/last_value", params=payload)
    assert response.status_code == 200, "Некорректный код ответа"
    dt_str = response.json()[1]["timestamp"]
    assert datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S') == datetime(2021, 7, 15, 10, 0, 0), "Некорректный timestamp"
    assert response.json()[1][payload["sensor_id"]] == 1.943, "Некорректное значение запрашиваемого датчика"


def test_unexist_sensor(clear_db, telemetry_fixture, script_execute):
    """GET запрос несуществующего в базе сенсора"""
    payload = {"sensor_id": "sensor100"}
    response = requests.get(f"{URL}/last_value", params=payload)
    assert response.status_code == 400, "Некорректный код ответа"
    assert f'column "sensor100" does not exist' in response.json()['detail'], "Некорректное описание ошибки"


def test_post_smoke(clear_db):
    """Корректная работа метода POST"""
    body = {
        "timestamp": "2021-07-15 10:36:54",
        "sensor_values": [
            {"sensor_id": "sensor1", "value": 1.074},
            {"sensor_id": "sensor2", "value": 54.295}
        ]
    }
    response = requests.post(f"{URL}/write_values", json=body)
    assert response.status_code == 200, "Некорректный код ответа"
    sql_query = """
                SELECT EXISTS (
                    SELECT *
                    FROM sensor_value
                    WHERE sensor1 = 1.074);
        """
    assert query_execute(sql_query), "Значение целевого датчика не было записано в БД"
    assert response.json()[1]['desc'] == "Показания следующих датчиков {'sensor2'} не были импортированы"


def test_ts_outofrange():
    """timestamp POST запроса мыньше чем максимальная дата в БД"""
    pass
