# escape=\
FROM ubuntu:22.04
LABEL maintainer="avisca@my.bcit.ca"
# Install dependencies
RUN apt update -y && \
    apt install -y apt-utils && \
    apt install -y wait-for-it && \
    apt install -y python3 python3-pip
# Cache requirements
COPY ./requirements.txt /src/requirements.txt
# Install application
WORKDIR /src
RUN pip3 install -r requirements.txt
# Copy files to workdir
COPY .  .
# Enable shell script
RUN chmod +x ./app-entrypoint.sh
# Program entrypoint
ENTRYPOINT [ "app-entrypoint.sh" ]
CMD [ "python3", "app.py" ]