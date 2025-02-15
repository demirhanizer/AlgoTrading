FROM python:3.10

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt --progress-bar=on

#RUN pip install schedule

# ✅ Ensure logs directory is writable
RUN mkdir -p /app/logs && chmod -R 777 /app/logs

COPY . .

EXPOSE 8000

CMD ["python", "execute.py"]
