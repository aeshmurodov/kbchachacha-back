# Используем стабильную версию Python
FROM python:3.11-slim

# Устанавливаем рабочую директорию в контейнере
WORKDIR /app

# Устанавливаем системные зависимости (нужны для сборки некоторых библиотек Python)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Копируем файл зависимостей
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь код приложения в контейнер
COPY . .

# Открываем порт, который указан в вашем .env (MAIN_WEBHOOK_LISTENING_PORT)
EXPOSE 8080

# Команда для запуска бота
# Замените main.py на имя вашего главного файла (например, app.py или run.py)
CMD ["python", "-u", "main.py"]