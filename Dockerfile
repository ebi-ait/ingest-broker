FROM python:3.6-slim
MAINTAINER Simon Jupp "jupp@ebi.ac.uk"

RUN mkdir /app
WORKDIR /app/

RUN apt-get update && \
    apt-get install -y git

COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

COPY broker /app/broker
COPY broker_app.py /app/broker_app.py

ENV INGEST_API=http://localhost:8080
ENV REQUESTS_MAX_RETRIES=5

EXPOSE 5000
ENTRYPOINT ["python"]
CMD ["broker_app.py"]
