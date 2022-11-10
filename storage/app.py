import connexion
from connexion import NoContent
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from data.base import Base
from data.readings import Temperature, Environment
from datetime import datetime
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

# Endpoints
def root() -> None:
    return NoContent, 204

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
    # logger.debug(f"Trace-ID: {body['trace_id']}")
    return NoContent, 201

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
    # logger.debug(f"Trace-ID: {body['trace_id']}")
    return NoContent, 201

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


app = connexion.FlaskApp(__name__, specification_dir='openapi/')
app.add_api('openapi.yml', strict_validation=True, validate_responses=True)


if __name__ == '__main__':
    app.run(port=8090, debug=False)