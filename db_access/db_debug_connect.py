import os

import psycopg2
from dotenv import load_dotenv
import pyodbc

load_dotenv()

# загрузка переменных окружения из .env файла для подключения к БД
server = 'localhost'
db = os.getenv('DB_NAME', default='DB')
username = os.getenv('POSTGRES_USER')
psswrd = os.getenv('POSTGRES_PASSWORD')


def psy_connection():
    """Отладочная функция для проверки работы psycopg2"""
    cnxn = psycopg2.connect(
        host=server,
        database=db,
        user=username,
        password=psswrd,
    )
    with cnxn:
        cursor = cnxn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sensor_value(
                    id SERIAL PRIMARY KEY,
                    sensor CHAR(15) NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    value REAL NOT NULL
                    );
            """)


def odbc_connection():
    """
    Отладочная функция для проверки работы pyodbc.
    Требуется установка драйвера https://www.postgresql.org/ftp/odbc/versions/msi/
    """
    cnxn = pyodbc.connect(
        'DRIVER={PostgreSQL Unicode};'
        f'SERVER={server};'
        f'DATABASE={db};'
        'ENCRYPT=yes;'
        f'UID={username};'
        f'PWD={psswrd};')
    with cnxn:
        cursor = cnxn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS t (
                id INT PRIMARY KEY,
                name VARCHAR NOT NULL
            )""")


def search_pg_drivers():
    """Поиск доступных драйверов Postgres для поля DRIVER={} метода pyodbc.connect"""
    driver_names = [x for x in pyodbc.drivers() if 'Postgre' in x]
    print(driver_names)


def search_drivers():
    """Поиск всех доступных драйверов для работы с БД"""
    driver_names = [x for x in pyodbc.drivers()]
    print(driver_names)


if __name__ == '__main__':
    search_drivers()
