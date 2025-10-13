FROM python:3.9-slim-bookworm

WORKDIR /app

RUN apt-get update && \
    apt-get install -y gcc g++ libssl-dev librdkafka-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY ./producer/requirements.txt /app/producer/requirements.txt
RUN pip install --no-cache-dir -r /app/producer/requirements.txt

COPY ./producer /app/producer
COPY ./config /app/config

CMD ["python", "-u", "producer/src/__main__.py"]
