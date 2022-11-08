# Создать образ на основе базового слоя python (там будет ОС и интерпретатор Python).
# 3.9 — используемая версия Python.
# slim — обозначение того, что образ имеет только необходимые компоненты для запуска,
# он не будет занимать много места при развёртывании.
FROM python:3.9-slim

# Сделать директорию /app рабочей директорией. Команда для внутренностей контейнреа
WORKDIR /json_import
COPY requirements-scheduler.txt /json_import/requirements-scheduler.txt

# Выполнить установку зависимостей внутри контейнера и обновить pip
RUN pip install --upgrade pip && \
    pip install -r /json_import/requirements-scheduler.txt --no-cache-dir


# Скопировать содержимое директории api, data, db_access, json_import c локального компьютера
# в директорию /app.
COPY . /json_import

# Выполнить запуск сервера FastAPI при старте контейнера.
CMD ["python", "task_scheduler.py"]
