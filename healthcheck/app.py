# Copyright 2020 - 2023 Alexander Visca. All rights reserved.
"""
Healthcheck Service

Polls all services for status. Forwards to a user interface service.

Environment configuration
"""
import connexion
import json
import logging
import logging.config
import os
import requests
import sqlite3
import time
import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from connexion import NoContent
from datetime import datetime
from data import Base, Health, sqlite_client, create_table, version
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Constants
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# Environment config
if 'TARGET_ENV' in os.environ and os.environ['TARGET_ENV'] == 'prod':
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

logger = logging.getLogger('healthcheck')

# application config
with open(app_conf_file, mode='r') as file:
    app_config = yaml.safe_load(file.read())

FQDN_URL = app_config['localhost']['fqdn']
DATA_URL = app_config['datastore']['filename']
INTERVAL = app_config['scheduler']['period_sec']
TIMEOUT = app_config['connection']['timeout']

DB_ENGINE = create_engine(f"sqlite:///{DATA_URL}")
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

# endpoints
def get_health() -> dict:
    data = query_db()
    status = {
        "system": data['system'], 
        "receiver": data['receiver'], 
        "storage": data['storage'], 
        "audit_log": data['audit_log'], 
        "processing": data['processing'], 
        "last_updated": data['last_updated']
        }
    return status, 200

# Poll services
def check_(service):
    try:
        res = requests.get(
            f'{FQDN_URL}/{service}/health', 
            timeout=TIMEOUT
            )
        res.raise_for_status()
        if res.status_code == 200:
            logger.info(f"{service} status: up - {res.text}")
            msg = json.loads(res.text)
            response = {"message": msg['message'], "status_code": res.status_code}
            
            return response
        else:
            return None

    except requests.exceptions.ReadTimeout as timeout_err:
        logger.error(f"{service} - Timeout ({TIMEOUT}) exceeded. {timeout_err}")
        return None
    
    except requests.exceptions.ConnectionError as err:
        logger.error(f"{service} - Connection failed. {err}")
        return None

    except requests.exceptions.RequestException as e:
        logger.error(f"{service} - {e}")
        return None

# health check
def check_health():
    logger.info(f"Checking health of services")
    system = []
    services = [
        "receiver", 
        "storage", 
        "audit_log", 
        "processing"
        ]
    status = {
        "system": str | None, 
        "receiver": str | None, 
        "storage": str | None, 
        "audit_log": str | None, 
        "processing": str | None,
        "last_updated": str | None
        }
    for service in services:
        res = check_(service)
        if res == None:
            status[service] = "down"
            system.append(0)
        else:
            status[service] = "running"
            system.append(1)
    
    if all(system):
        status['system'] = "green"
        logger.info(f"System status: Green")
    elif any(system):
        status['system'] = "yellow"
        logger.warning(f"System status: Yellow")
    elif not all(system):
        status['system'] = "red"
        logger.critical(f"System status: Red")
    
    status['last_updated'] = datetime.strftime(datetime.now(), DATETIME_FORMAT)
    insert_(status)

# database utilities
def query_db():
    session = DB_SESSION()
    result = session.query(Health).order_by(Health.last_updated.desc()).first()
    status = {
        "system": result.system, 
        "receiver": result.receiver, 
        "storage": result.storage, 
        "audit_log": result.audit_log, 
        "processing": result.processing,
        "last_updated": result.last_updated.strftime(DATETIME_FORMAT)
        }
    session.close()

    return status

def insert_(data: dict) -> None:
    session = DB_SESSION()
    status = Health(
        system=data['system'], 
        receiver=data['receiver'], 
        storage=data['storage'], 
        audit_log=data['audit_log'], 
        processing=data['processing'], 
        last_updated=datetime.strptime(data['last_updated'], DATETIME_FORMAT)
        )
    session.add(status)
    session.commit()

    session.close()

def init_database(filename: str):
    init_msg = "starting.."
    status = {
        "system": init_msg, 
        "receiver": init_msg, 
        "storage": init_msg, 
        "audit_log": init_msg, 
        "processing": init_msg,
        "last_updated": datetime.strftime(datetime.now(), DATETIME_FORMAT)
        }
    abs_path = os.path.join(os.path.dirname(__file__), filename)
    if not os.path.exists(abs_path):
        sqlite_client(filename, create_table)
        insert_(status)
        logger.info(f"Connected to {filename}")
    elif os.path.exists(abs_path):
        logger.info(f"Connected to {filename}")


def init_scheduler(interval: int) -> None:
    sched = BackgroundScheduler(daemon=True, job_defaults={'max_instances': 3})
    sched.add_job(check_health, 'interval', seconds=interval)
    sched.start()

app = connexion.FlaskApp(__name__, specification_dir='openapi/')
app.add_api('openapi.yml', strict_validation=True, validate_responses=True)


def main() -> None:
    init_database(DATA_URL)
    init_scheduler(INTERVAL)
    app.run(port=8120, debug=False)


if __name__ == '__main__':
    main()
