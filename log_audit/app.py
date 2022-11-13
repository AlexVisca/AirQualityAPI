import connexion
from connexion import NoContent
from pykafka import KafkaClient
from pykafka.common import OffsetType
from pykafka.exceptions import KafkaException, SocketDisconnectedError
import json
import requests
import time
import yaml
import logging
import logging.config


# Logging config
with open('config/log_conf.yml', mode='r') as file:
    log_config = yaml.safe_load(file.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('auditlog')

# application config
with open('config/app_conf.yml', mode='r') as file:
    app_config = yaml.safe_load(file.read())

SERVER_HOST = app_config['server']['host']
SERVER_PORT = app_config['server']['port']
SERVER_URI = app_config['events']['topic']

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

def main() -> None:
    app.run(port=8110, debug=False)


if __name__ == '__main__':
    main()