FROM python:3.11-slim

WORKDIR /app

# Создаём папку для постоянного хранилища
RUN mkdir -p /data

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Объявляем /data как volume для сохранения данных между деплоями
VOLUME ["/data"]

CMD ["python", "bot.py"]

