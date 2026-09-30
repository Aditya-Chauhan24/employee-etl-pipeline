FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline.py .
COPY input/ ./input/

RUN mkdir -p logs output

CMD ["python", "pipeline.py"]
