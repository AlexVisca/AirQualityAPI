# Copyright 2020 - 2023 Alexander Visca. All rights reserved
"""
Storage Service

Receives sensor telemetry from message broker service.
Stores telemetry data in a MySQL database service.

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
from data.base import Base, connect, create_temp, create_envr
from data.readings import Temperature, Environment
from datetime import datetime
from os import environ
from pykafka import KafkaClient
from pykafka.common import OffsetType
from pykafka.exceptions import KafkaException, SocketDisconnectedError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from threading import Thread

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

logger = logging.getLogger('database')

# application config
with open(app_conf_file, mode='r') as file:
    app_config = yaml.safe_load(file.read())

SERVER_HOST = app_config['server']['host']
SERVER_PORT = app_config['server']['port']
DATA_TOPIC = app_config['events']['topic']

DB_USER = app_config['datastore']['username']
DB_PASS = app_config['datastore']['password']
DB_HOST = app_config['datastore']['host']
DB_PORT = app_config['datastore']['port']
DB_NAME = app_config['datastore']['db']

DB_ENGINE = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

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
    logger.info(f"Received temperature telemetry -- trace ID: {body['trace_id']}")
    
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
    logger.info(f"Received environment telemetry -- trace ID: {body['trace_id']}")

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
    topic = connect_kafka_client(max_retries=3, timeout=2)

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
                
            if msg['type'] == 'environment':
                environment(payload)
                
            consumer.commit_offsets()

    except SocketDisconnectedError as e:
        consumer.stop()
        consumer.start()

# Server connection
def connect_kafka_client(max_retries: int, timeout: int):
    count = 0
    while count < max_retries:
        try:
            client = KafkaClient(hosts=f'{SERVER_HOST}:{SERVER_PORT}')
            topic = client.topics[str.encode(DATA_TOPIC)]

            return topic

        except KafkaException as e:
            logger.error(f"Connection failed - {e} - Retries ({count})")
            time.sleep(timeout)
            count += 1
            continue

    else:
        logger.error(f"Connection failed - Unable to connect to kafka server. Max retries exceeded ({max_retries})")
        raise SystemExit(1)

# Database utilities
def init_db(database, connection, cursor):
    try:
        cursor.execute('''SHOW VARIABLES like 'version';''')
        version = cursor.fetchone()
        logger.info(f"Connected to {database} database - MySQL {version[0]} {version[1]}")
        # 
    except Exception as e:
        raise

def _tables(database, connection, cursor):
    cursor.execute('''SHOW TABLES;''')
    tables = cursor.fetchall()
    try:
        temp = tables[-1]
        envr = tables[-2]
        logger.info(f"{database} tables: {temp[0]}, {envr[0]}")
        # 
    except IndexError:
        logger.info(f"Creating tables")
        cursor.execute(create_temp)
        logger.info("Created table `temperature`")
        cursor.execute(create_envr)
        logger.info("Created table `environment`")
        connection.commit()
        # 
    except Exception as e:
        raise

# Database connection
def connect_database(user: str, password: str, host: str, port: int, database: str):
    with connect(user=user, password=password, host=host, port=port, database=database, auth_plugin='caching_sha2_password') as cnx:
        crs = cnx.cursor()
        try:
            init_db(database, cnx, crs)
            _tables(database, cnx, crs)
            # 
        except Exception as e:
            logger.error(str(e))

        finally:
            crs.close()


app = connexion.FlaskApp(__name__, specification_dir='openapi/')
app.add_api('openapi.yml', strict_validation=True, validate_responses=True)

def main() -> None:
    connect_database(user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT, database=DB_NAME)
    t1 = Thread(target=process_messages)
    t1.daemon
    t1.start()
    app.run(port=8090, debug=False)


if __name__ == '__main__':
    main()