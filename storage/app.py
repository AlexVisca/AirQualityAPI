import connexion
from connexion import NoContent
from pykafka import KafkaClient
from pykafka.common import OffsetType
from pykafka.exceptions import KafkaException, SocketDisconnectedError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from threading import Thread
from data.base import Base
from data.readings import Temperature, Environment
from datetime import datetime
import json
import time
import yaml
import logging
import logging.config

# Constants
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
# Logging config
with open('config/log_conf.yml', mode='r') as file:
    log_config = yaml.safe_load(file.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('database')
# Database config
with open('config/db_conf.yml', mode='r') as file:
    db_config = yaml.safe_load(file.read())

DB_ENGINE = create_engine(
    f"mysql+pymysql://" +
    f"{db_config['datastore']['username']}:" +
    f"{db_config['datastore']['password']}@" +
    f"{db_config['datastore']['host']}:" +
    f"{db_config['datastore']['port']}/" +
    f"{db_config['datastore']['db']}"
    )

Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

# application config
with open('config/app_conf.yml', mode='r') as file:
    app_config = yaml.safe_load(file.read())

SERVER_HOST = app_config['server']['host']
SERVER_PORT = app_config['server']['port']
SERVER_URI = app_config['events']['topic']

# Endpoints
def root() -> None:
    return NoContent, 204

def get_temperature(timestamp: str) -> list:
    timestamp_datetime = datetime.strptime(timestamp, DATETIME_FORMAT)

    session = DB_SESSION()
    readings = session.query(Temperature).filter(Temperature.date_created >= timestamp_datetime)

    results_list = list()
    for reading in readings:
        results_list.append(reading.to_dict())
    
    session.close()
    # logger.debug(f"Query for temperature after {timestamp_datetime} returns {len(results_list)}")
    return results_list, 200


def get_environment(timestamp: str) -> list:
    timestamp_datetime = datetime.strptime(timestamp, DATETIME_FORMAT)

    session = DB_SESSION()
    readings = session.query(Environment).filter(Environment.date_created >= timestamp_datetime)

    results_list = list()
    for reading in readings:
        results_list.append(reading.to_dict())
    
    session.close()
    # logger.debug(f"Query for environment after {timestamp_datetime} returns {len(results_list)}")
    return results_list, 200

# storage functions
def temperature(body) -> None:
    session = DB_SESSION()
    temp = Temperature(
        body['device_id'], 
        body['location'], 
        body['temperature'], 
        body['timestamp'], 
        body['trace_id']
    )
    session.add(temp)
    session.commit()

    session.close()
    
    return NoContent, 201

def environment(body) -> None:
    session = DB_SESSION()
    envr = Environment(
        body['device_id'],
        body['environment']['pm2_5'],
        body['environment']['co_2'],
        body['location'], 
        body['timestamp'], 
        body['trace_id']
    )
    session.add(envr)
    session.commit()

    session.close()
    
    return NoContent, 201

# message processor
def process_messages():
    topic = create_kafka_connection(max_retries=3, timeout=2)

    consumer = topic.get_simple_consumer(
        consumer_group=b'telemetry', 
        auto_offset_reset=OffsetType.LATEST, 
        reset_offset_on_start=False
    )
    
    try:
        for msg in consumer:
            msg_str = msg.value.decode('utf-8')
            msg = json.loads(msg_str)

            payload = msg['payload']
            if msg['type'] == 'temperature':
                temperature(payload)
                logger.info("stored temperature telemetry")
            if msg['type'] == 'environment':
                environment(payload)
                logger.info("stored environment telemetry")

            consumer.commit_offsets()

    except SocketDisconnectedError as e:
        consumer.stop()
        consumer.start()


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



app = connexion.FlaskApp(__name__, specification_dir='openapi/')
app.add_api('openapi.yml', strict_validation=True, validate_responses=True)

def main() -> None:
    t1 = Thread(target=process_messages)
    t1.daemon
    t1.start()
    app.run(port=8090, debug=False)


if __name__ == '__main__':
    main()