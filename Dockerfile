FROM python:3.9-slim

WORKDIR /app

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Копируем зависимости
COPY requirements.txt .

# Устанавливаем Python зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь проект
COPY . .

# Создаем папку для данных
RUN mkdir -p /app/data

# Делаем скрипты исполняемыми
RUN chmod +x scripts/wait-for-postgres.sh

# Открываем порт для Jupyter (опционально)
EXPOSE 8888

# Команда по умолчанию
CMD ["python", "app/load_csv.py", "--help"]