FROM python:3.10

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/logs && chmod -R 777 /app/logs

COPY . .

EXPOSE 8000

CMD ["python", "execute.py"]
