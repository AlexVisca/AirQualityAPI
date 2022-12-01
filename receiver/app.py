# Copyright 2020 - 2023 Alexander Visca. All rights reserved
"""
Receiver Service

Receives sensor telemetry from devices over HTTP as post resquests.
Forwards telemetry to a message broker service.

Environment configuration
SERVER_HOST (string):   URL of message broker service
SERVER_PORT (integer):  port for message broker service
DATA_TOPIC (string):    topic group assigned to data
"""
import connexion
import logging
import logging.config
import json
import time
import yaml
from connexion import NoContent
from datetime import datetime
from os import environ
from pykafka import KafkaClient
from pykafka.exceptions import SocketDisconnectedError, LeaderNotAvailable, KafkaException

# Constants
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

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

logger = logging.getLogger('receiver')

# application config
with open(app_conf_file, mode='r') as file:
    app_config = yaml.safe_load(file.read())

SERVER_HOST = app_config['server']['host']
SERVER_PORT = app_config['server']['port']
DATA_TOPIC = app_config['events']['topic']

# Endpoints
def root():
    logger.info("Received connection request from device")
    
    return NoContent, 204

def health():
    return {"message": "OK"}, 200

def temperature(body):
    location = body['location']
    trace = body['trace_id']
    # convert payload for kafka
    msg = {
        'type': 'temperature', 
        'datetime': datetime.now().strftime(DATETIME_FORMAT), 
        'payload': body
    }
    msg_str = json.dumps(msg)
    
    producer = topic.get_sync_producer()
    try:
        producer.produce(msg_str.encode('utf-8'))
    except (SocketDisconnectedError, LeaderNotAvailable) as e:
        logger.warning(f"Restarting producer - ERROR: {e}")
        producer = topic.get_sync_producer()
        producer.stop()
        producer.start()
        producer.produce(msg_str.encode('utf-8'))

    logger.info(f"Received temperature telemetry from device at {location} -- trace ID: {trace}")
    
    return NoContent, 201


def environment(body):
    location = body['location']
    trace = body['trace_id']
    # convert payload for kafka
    msg = {
        'type': 'environment', 
        'datetime': datetime.now().strftime(DATETIME_FORMAT), 
        'payload': body
    }
    msg_str = json.dumps(msg)

    producer = topic.get_sync_producer()
    try:
        producer.produce(msg_str.encode('utf-8'))
    except (SocketDisconnectedError, LeaderNotAvailable) as e:
        logger.warning(f"Restarting producer - ERROR: {e}")
        producer = topic.get_sync_producer()
        producer.stop()
        producer.start()
        producer.produce(msg_str.encode('utf-8'))

    logger.info(f"Received environment telemetry from device at {location} -- trace ID: {trace}")
    
    return NoContent, 201

# connect to kafka server
def create_kafka_connection(max_retries: int, timeout: int):
    count = 0
    while count < max_retries:
        try:
            client = KafkaClient(hosts=f'{SERVER_HOST}:{SERVER_PORT}')
            topic = client.topics[str.encode(DATA_TOPIC)]
            logger.info(f"Client connected to Kafka server")

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
app.add_api('openapi.yml', base_path='/receiver', strict_validation=True, validate_responses=True)


def main() -> None:
    app.run(port=8080, debug=False)


if __name__ == '__main__':
    main()
