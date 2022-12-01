# escape=\
FROM ubuntu:22.04
LABEL maintainer="avisca@my.bcit.ca"
# Install dependencies
RUN apt update -qy \
    && apt install -y python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*
# Cache requirements
COPY ./requirements.txt /src/requirements.txt
# Install application
WORKDIR /src
RUN pip3 install -r requirements.txt
# Copy files to workdir
COPY .  .
# Program entrypoint
ENTRYPOINT [ "python3" ]
CMD [ "app.py" ]