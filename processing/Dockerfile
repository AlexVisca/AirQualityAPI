# syntax=docker/dockerfile:1
FROM ubuntu:22.04

LABEL maintainer="avisca@my.bcit.ca"

RUN apt update -y && \
    apt install -y python3 python3-pip

COPY ./requirements.txt /src/requirements.txt

WORKDIR /src

RUN pip3 install -r requirements.txt

COPY .  /src

ENTRYPOINT [ "python3" ]

CMD [ "app.py" ]