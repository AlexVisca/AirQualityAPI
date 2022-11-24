# Copyright 2020 - 2023 Alexander Visca. All rights reserved
"""
Audit Service

Queries messages in queue from message broker service and
forwards messages to a user interface service.

Environment configuration
SERVER_HOST (string):   URL of message broker service
SERVER_PORT (integer):  port for message broker service
DATA_TOPIC (string):    topic group assigned to data
"""
import connexion
import logging
import logging.config
import json
import requests
import time
import yaml
from connexion import NoContent
from flask_cors import CORS, cross_origin
from os import environ
from pykafka import KafkaClient
from pykafka.common import OffsetType
from pykafka.exceptions import KafkaException, SocketDisconnectedError

# Environment config
if 'TARGET_ENV' in environ and environ['TARGET_ENV'] == 'prod':
    logging.info("ENV: Production")
    app_conf_file = '/config/app_conf.yml'
    log_conf_file = '/config/log_conf.yml'
else:
    logging.info("ENV: Development")
    app_conf_file = 'app_conf.yml'
    log_conf_file = 'log_conf.yml'

# Logging config
with open(log_conf_file, mode='r') as file:
    log_config = yaml.safe_load(file.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('auditlog')

# application config
with open(app_conf_file, mode='r') as file:
    app_config = yaml.safe_load(file.read())

SERVER_HOST = app_config['server']['host']
SERVER_PORT = app_config['server']['port']
DATA_TOPIC = app_config['events']['topic']

# endpoints
def get_temperature(index):
    consumer = topic.get_simple_consumer(
        reset_offset_on_start=True, 
        consumer_timeout_ms=1000
    )
    try:
        count = 0
        while count < index + 1:
            for msg in consumer:
                msg_str = msg.value.decode('utf-8')
                msg = json.loads(msg_str)
                
                if msg['type'] == 'temperature':
                    payload = msg
                    count += 1
                
                elif msg['type'] == 'environment':
                    continue

        return payload, 200

    except:
        logger.error("No more messages found")
    
    logger.error(f"Could not find temperature at index: {index}")
    return { "message": "Not Found" }, 404


def get_environment(index):
    consumer = topic.get_simple_consumer(
        reset_offset_on_start=True, 
        consumer_timeout_ms=1000
    )
    try:
        count = 0
        while count < index + 1:
            for msg in consumer:
                msg_str = msg.value.decode('utf-8')
                msg = json.loads(msg_str)
                
                if msg['type'] == 'environment':
                    payload = msg
                    count += 1
                
                elif msg['type'] == 'temperature':
                    continue

        return payload, 200

    except:
        logger.error("No more messages found")
    
    logger.error(f"Could not find environment at index: {index}")
    return { "message": "Not Found" }, 404

# debug function (unmapped)
def get_queue(consumer):
    temp_queue = dict()
    index = 0
    try:
        for msg in consumer:
            msg_str = msg.value.decode('utf-8')
            msg = json.loads(msg_str)
            temp_queue[index] = msg
    
    except SocketDisconnectedError as e:
        consumer.stop()
        consumer.start()

    logger.debug(temp_queue)
    return NoContent, 200


def create_kafka_connection(max_retries: int, timeout: int):
    count = 0
    while count < max_retries:
        try:
            client = KafkaClient(hosts=f'{SERVER_HOST}:{SERVER_PORT}')
            topic = client.topics[str.encode(DATA_TOPIC)]

            return topic

        except KafkaException as e:
            logger.error(f"Connection failed - {e} - Retries: ({count})")
            time.sleep(timeout)
            count += 1
            continue

    else:
        logger.error(f"Connection failed - Unable to connect to kafka server. Max retries exceeded ({max_retries})")
        raise SystemExit(1)

topic = create_kafka_connection(max_retries=3, timeout=2)

app = connexion.FlaskApp(__name__, specification_dir='openapi/')
CORS(app.app)
app.app.config['CORS_HEADERS'] = 'Content-Type'
app.add_api('openapi.yml', strict_validation=True, validate_responses=True)

def main() -> None:
    app.run(port=8110, debug=False)


if __name__ == '__main__':
    main()