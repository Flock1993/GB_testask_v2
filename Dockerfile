# Создать образ на основе базового слоя python (там будет ОС и интерпретатор Python).
# 3.9 — используемая версия Python.
# slim — обозначение того, что образ имеет только необходимые компоненты для запуска,
# он не будет занимать много места при развёртывании.
FROM python:3.9-slim

# Сделать директорию /app рабочей директорией.
WORKDIR /app/api/
COPY requirements.txt /app/requirements.txt

# Выполнить установку зависимостей внутри контейнера и обновить pip
RUN pip install --upgrade pip && \
    pip install -r /app/requirements.txt --no-cache-dir


# Скопировать содержимое директории api, data, db_access, json_import c локального компьютера
# в директорию /app.
COPY . /app

# Выполнить запуск сервера FastAPI при старте контейнера.
CMD ["uvicorn", "main:app", "--reload"]
