FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY db_insert_demo.py .

CMD ["python", "db_insert_demo.py"]
