# Copyright 2020 - 2023 Alexander Visca. All rights reserved
"""
Processing Service

Receives sensor telemetry data from storage service. 
Processes data for statistical analysis and 
forwards to a user interface service.

Environment configuration
SERVER_URL (string):    URL of storage service
INTERVAL (integer):     Interval (seconds) between requesting data
TIMEOUT (integer):      Timeout (seconds) to wait for response
"""
import connexion
import logging
import logging.config
import json
import requests
import time
import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from connexion import NoContent
from datetime import datetime
from os import environ
from base import Base
from stats import Stats
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Constants
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# Environment config
if 'TARGET_ENV' in environ and environ['TARGET_ENV'] == 'prod':
    app_conf_file = 'config/app_conf.yml'
    log_conf_file = 'config/log_conf.yml'
else:
    app_conf_file = 'app_conf.yml'
    log_conf_file = 'log_conf.yml'

# Logging config
with open(log_conf_file, mode='r') as file:
    log_config = yaml.safe_load(file.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('processor')

# application config
with open(app_conf_file, mode='r') as file:
    app_config = yaml.safe_load(file.read())

SERVER_URL = app_config['eventstore']['url']
INTERVAL = app_config['scheduler']['period_sec']
TIMEOUT = app_config['connection']['timeout']

DB_ENGINE = create_engine(
    f"sqlite:///{app_config['datastore']['filename']}"
    )
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

# Endpoints
def get_stats() -> dict:
    data = query_db()
    stats = {
        'max_temp': data['max_temp'], 
        'min_temp': data['min_temp'], 
        'avg_temp': data['avg_temp'], 
        'max_pm2_5': data['max_pm2_5'], 
        'max_co_2': data['max_co_2']
    }
    return stats, 200

# processor logic
def populate_stats() -> None:
    logger.info("Started periodic processing")
    # read in stats from sqlite db
    stats = query_db()
    # last updated statistics
    last_buffer = stats['temp_buffer']
    last_max = stats['max_temp']
    last_min = stats['min_temp']
    last_max_pm25 = stats['max_pm2_5']
    last_max_co_2 = stats['max_co_2']
    last_timestamp = stats['last_updated']
    # Current timestamp
    timestamp = datetime.strftime(datetime.now(), DATETIME_FORMAT)
    # Query storage server endpoints using timestamp
    # Temperature table
    temp_res = requests.get(
        f"{SERVER_URL}/temperature", 
        params={'timestamp': last_timestamp}
        )
    temp_table_contents = json.loads(temp_res.text)
    logger.info(f"Response received from database. Status code: {temp_res.status_code}, content: {len(temp_table_contents)}")
    # Environment table
    env_res = requests.get(
        f"{SERVER_URL}/environment", 
        params={'timestamp': last_timestamp}
        )
    env_table_contents = json.loads(env_res.text)
    logger.info(f"Response received from database. Status code: {env_res.status_code}, content: {len(env_table_contents)}")
    # Parse updated telemetry
    try:
        last_temp_packet = temp_table_contents[-1]
        count = last_temp_packet['id']
        # Temperature telemetry
        temp_list = list()
        temp_buffer = float()
        for packet in temp_table_contents:
            temp_list.append(packet['temperature'])
            temp_buffer += packet['temperature']
        new_buffer = last_buffer + temp_buffer
        # Environment telemetry
        pm25_list = list()
        co2_list = list()
        for packet in env_table_contents:
            pm25_list.append(packet['environment']['pm2_5'])
            co2_list.append(packet['environment']['co_2'])
        # Update stats
        payload = {
            'count': count, 
            'temp_buffer': new_buffer, 
            'max_temp': max(last_max, max(temp_list, default=-22)), 
            'min_temp': min(last_min, min(temp_list, default=52)), 
            'avg_temp': round(new_buffer/count, 2), 
            'max_pm2_5': max(last_max_pm25, max(pm25_list, default=0)), 
            'max_co_2': max(last_max_co_2, max(co2_list, default=0)), 
            'last_updated': timestamp
        }
        # Add new row to database
        insert_db(payload)
        logger.info("Database updated with latest telemetry")

    except IndexError:
        logger.info("Telemetry is up to date")
    
    logger.info("Stopped periodic processing")

# Processer sub-functions
def query_db() -> dict:
    session = DB_SESSION()
    result = session.query(Stats).order_by(Stats.last_updated.desc()).first()
    payload = {
        'count': result.count, 
        'temp_buffer': result.temp_buffer, 
        'max_temp': result.max_temp, 
        'min_temp': result.min_temp, 
        'avg_temp': result.avg_temp, 
        'max_pm2_5': result.max_pm2_5, 
        'max_co_2': result.max_co_2, 
        'last_updated': result.last_updated.strftime(DATETIME_FORMAT)
    }
    session.close()
    
    return payload

def insert_db(data: dict) -> None:
    session = DB_SESSION()
    stats = Stats(
        count=data['count'], 
        temp_buffer=data['temp_buffer'], 
        max_temp=data['max_temp'], 
        min_temp=data['min_temp'], 
        avg_temp=data['avg_temp'], 
        max_pm2_5=data['max_pm2_5'], 
        max_co_2=data['max_co_2'], 
        last_updated=datetime.strptime(data['last_updated'], DATETIME_FORMAT)
    )
    session.add(stats)
    session.commit()

    session.close()

def init_db() -> None:
    # populate first row with default stats
    session = DB_SESSION()
    stats = Stats(count=0, temp_buffer=0, 
        max_temp=-21, min_temp=51, avg_temp=0, 
        max_pm2_5=0, max_co_2=0, 
        last_updated=datetime.now()
    )
    session.add(stats)
    session.commit()
    
    session.close()

# Server connection
def create_connection(url: str, timeout: int) -> None:
    retries: int = 0
    while retries < timeout:
        try:
            res = requests.head(
                url=url
            )
            logger.info(f"Connected to server at {url} - {res.status_code}")
            break

        except requests.exceptions.ConnectionError as err:
            logger.warning(f"Unable to connect to server. Error: {err}")
            retries += 1
            time.sleep(2)
            continue
        
        except requests.exceptions.RequestException as e:
            logger.error(e)
            raise SystemExit(1)
    
    else:
        logger.error(f"Unable to connect to server at {url}. Max retries exceeded ({retries})")
        raise SystemExit(1)

def init_scheduler() -> None:
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats, 
        'interval', 
        seconds=INTERVAL
        )
    sched.start()


app = connexion.FlaskApp(__name__, specification_dir='openapi/')
app.add_api('openapi.yml', strict_validation=True, validate_responses=True)

def main() -> None:
    init_db()
    create_connection(SERVER_URL, TIMEOUT)
    init_scheduler()
    app.run(
        port=8100, 
        debug=False)


if __name__ == '__main__':
    main()
