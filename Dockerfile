# Dockerfile 
FROM python:3.11-slim-bullseye

WORKDIR /app

# Copy Dependencies 
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt 

# Copy application code 
COPY . .

# Default command ( can be overridden)
CMD ["python3", "etl_kafka_producer.py"]