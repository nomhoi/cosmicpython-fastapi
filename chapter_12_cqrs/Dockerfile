FROM python:3.7-slim-buster
RUN apt-get update \
&& apt-get install gcc -y \
&& apt-get clean
COPY requirements.txt /tmp
RUN pip install -r /tmp/requirements.txt

RUN mkdir -p /src
COPY src/ /src/
RUN pip install -e /src
COPY tests/ /tests/

WORKDIR /src
