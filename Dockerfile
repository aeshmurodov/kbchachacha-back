FROM python:3.11-slim

# Настройки Python и Poetry
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    POETRY_VERSION=1.8.2 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_NO_INTERACTION=1

# Устанавливаем рабочую директорию
WORKDIR /app

# Устанавливаем системные зависимости (часто нужны для сборки asyncpg и других пакетов)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем Poetry
RUN pip install "poetry==$POETRY_VERSION"

# Сначала копируем ТОЛЬКО файлы зависимостей.
# Это позволяет Docker закешировать шаг установки зависимостей, 
# если в pyproject.toml / poetry.lock ничего не менялось.
COPY pyproject.toml poetry.lock* ./

# Устанавливаем зависимости (без dev-зависимостей, только прод)
RUN poetry install --without dev --no-root

# Теперь копируем весь остальной код приложения
COPY . .

# Открываем порт для вебхуков
EXPOSE 8080

# Запускаем бота (замените main.py на ваш главный файл)
CMD ["python", "main.py"]