FROM quay.io/ebi-ait/ingest-base-images:python_3.10-slim
LABEL maintainer="hca-ingest-dev@ebi.ac.uk"

RUN mkdir /app
WORKDIR /app/

RUN pip install --upgrade pip
RUN pip install pip-tools

COPY requirements.txt /app/requirements.txt
RUN pip-sync /app/requirements.txt

COPY broker /app/broker
COPY broker_app.py /app/broker_app.py
COPY logging-config.json /app/logging-config.json

ENV INGEST_API=http://localhost:8080
ENV REQUESTS_MAX_RETRIES=5

EXPOSE 5000
ENTRYPOINT ["python"]
CMD ["broker_app.py"]
