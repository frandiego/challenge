ARG IMAGE_VARIANT=slim-buster
ARG OPENJDK_VERSION=8
ARG PYTHON_VERSION=3.9.8

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3

FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}
RUN apt-get update && apt-get install -y unzip
COPY --from=py3 / /

WORKDIR /opt/app
ENV PYTHONPATH .
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY config.yml .
COPY input_schema.json .
COPY output_schema.json .
COPY src/ src/
COPY main.py .

ARG INPUP
COPY $INPUP .
RUN unzip -o -qq input-example.zip
RUN mkdir -p output