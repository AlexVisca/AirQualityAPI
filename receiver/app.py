import connexion
from connexion import NoContent
from pykafka import KafkaClient
from pykafka.exceptions import KafkaException, SocketDisconnectedError, LeaderNotAvailable
from datetime import datetime
import json
import time
import yaml
import logging
import logging.config

# Constants
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
# Logging config variables
with open('config/log_conf.yml', mode='r') as file:
    log_config = yaml.safe_load(file.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('receiver')

# App config variables
with open('config/app_conf.yml', mode='r') as file:
    app_config = yaml.safe_load(file.read())

SERVER_HOST = app_config['server']['host']
SERVER_PORT = app_config['server']['port']
SERVER_URI = app_config['events']['topic']

# Endpoints
def root():
    return NoContent, 204

def temperature(body):
    payload = {
        'device_id': body['device_id'], 
        'location': body['location'], 
        'temperature': body['temperature'], 
        'timestamp': body['timestamp'], 
        'trace_id': body['trace_id']
    }
    # convert payload for kafka
    msg = {
        'type': 'temperature', 
        'datetime': datetime.now().strftime(DATETIME_FORMAT), 
        'payload': payload
    }
    msg_str = json.dumps(msg)
    
    producer = topic.get_sync_producer()
    try:
        producer.produce(msg_str.encode('utf-8'))
    except (SocketDisconnectedError, LeaderNotAvailable) as e:
        producer = topic.get_sync_producer()
        producer.stop()
        producer.start()
        producer.produce(msg_str.encode('utf-8'))

    return NoContent, 201


def environment(body):
    payload = {
        'device_id': body['device_id'], 
        'environment': {
            'co_2': body['environment']['co_2'], 
            'pm2_5': body['environment']['pm2_5'], 
        }, 
        'location': body['location'], 
        'timestamp': body['timestamp'], 
        'trace_id': body['trace_id']
    }
    # convert payload for kafka
    msg = {
        'type': 'environment', 
        'datetime': datetime.now().strftime(DATETIME_FORMAT), 
        'payload': payload
    }
    msg_str = json.dumps(msg)

    producer = topic.get_sync_producer()
    try:
        producer.produce(msg_str.encode('utf-8'))
    except (SocketDisconnectedError, LeaderNotAvailable) as e:
        producer = topic.get_sync_producer()
        producer.stop()
        producer.start()
        producer.produce(msg_str.encode('utf-8'))

    return NoContent, 201

# connect to kafka server
def create_kafka_connection(max_retries: int, timeout: int):
    count = 0
    while count < max_retries:
        try:
            client = KafkaClient(hosts=f'{SERVER_HOST}:{SERVER_PORT}')
            topic = client.topics[str.encode(SERVER_URI)]

            return topic

        except KafkaException as e:
            logger.error(f"Connection failed - {e}")
            time.sleep(timeout)
            count += 1
            continue

    else:
        logger.error(f"Connection failed - Unable to connect to kafka server. Max retries exceeded")
        raise SystemExit

topic = create_kafka_connection(max_retries=3, timeout=2)

app = connexion.FlaskApp(__name__, specification_dir='openapi/')
app.add_api('openapi.yml', strict_validation=True, validate_responses=True)


if __name__ == '__main__':
    app.run(port=8080, debug=False)