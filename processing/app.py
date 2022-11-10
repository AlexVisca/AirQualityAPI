import connexion
from connexion import NoContent
from apscheduler.schedulers.background import BackgroundScheduler

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from data.base import Base
from data.stats import Stats

from datetime import datetime
import requests
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

logger = logging.getLogger('processer')
# App config
with open('config/app_conf.yml', mode='r') as file:
    app_config = yaml.safe_load(file.read())

DB_ENGINE = create_engine(f"sqlite:///{app_config['datastore']['filename']}")
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

SERVER_URL: str = app_config['eventstore']['url']
INTERVAL: int = app_config['scheduler']['period_sec']
TIMEOUT: int = app_config['connection']['timeout']

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


# Processer Logic
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
            'max_temp': max(last_max, max(temp_list)), 
            'min_temp': min(last_min, min(temp_list)), 
            'avg_temp': round(new_buffer/count, 2), 
            'max_pm2_5': max(last_max_pm25, max(pm25_list)), 
            'max_co_2': max(last_max_co_2, max(co2_list)), 
            'last_updated': timestamp
        }
        # Add new row to database
        insert_db(payload)
        logger.info("Database updated with latest telemetry")

    except IndexError:
        logger.info("Telemetry is up to date")
    
    logger.info("Stopped periodic processing")

def init_scheduler() -> None:
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats, 
        'interval', 
        seconds=INTERVAL
        )
    sched.start()

# Server connection
def establish_connection(url: str, timeout: int) -> None:
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
            raise SystemExit(e)
    
    else:
        logger.error(f"Unable to connect to server at {url}. Max retries exceeded")
        raise SystemExit()


app = connexion.FlaskApp(__name__, specification_dir='openapi/')
app.add_api('openapi.yml', strict_validation=True, validate_responses=True)


if __name__ == '__main__':
    establish_connection(SERVER_URL, TIMEOUT)
    init_scheduler()
    app.run(port=8100, debug=True)
