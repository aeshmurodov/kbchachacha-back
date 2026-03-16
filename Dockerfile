FROM python:3.11-slim

# Настройки Python и Poetry (убрали жесткую версию POETRY_VERSION)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_NO_INTERACTION=1

# Устанавливаем рабочую директорию
WORKDIR /app

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем актуальную версию Poetry
RUN pip install poetry

# Копируем конфигурацию зависимостей
COPY pyproject.toml poetry.lock* ./

# Устанавливаем зависимости
# В новых версиях Poetry --no-root переименовали/улучшили, но команда всё ещё работает
RUN poetry install --without dev --no-root

# Копируем остальной код
COPY . .

# Открываем порт для вебхуков
EXPOSE 8080

# Команда для запуска (убедитесь, что файл называется main.py)
CMD["python", "main.py"]