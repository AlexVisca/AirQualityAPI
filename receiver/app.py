import connexion
from connexion import NoContent
from datetime import datetime
import requests
import yaml
import logging
import logging.config


# Logging config variables
with open('config/log_conf.yml', mode='r') as file:
    log_config = yaml.safe_load(file.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('receiver')

# App config variables
with open('config/app_conf.yml', mode='r') as file:
    app_config = yaml.safe_load(file.read())

SERVER_URL = app_config['eventstore']['url']
SERVER_HOST = app_config['server']['host']
SERVER_PORT = app_config['server']['port']

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
    try:
        res = requests.post(
            url=f"{SERVER_URL}temperature", 
            json=payload
            )
        # logger.debug(f"Event trace: {body['trace_id']}")
        return NoContent, 202
        
    except requests.exceptions.ConnectionError as err:
        logger.warning(f"Unable to connect to server. Error: {err}")
        return 'Bad Gateway', 502


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
    try:
        res = requests.post(
            url=f"{SERVER_URL}environment", 
            json=payload
            )
        # logger.debug(f"Event trace: {body['trace_id']}")
        return NoContent, 202

    except requests.exceptions.ConnectionError as err:
        logger.warning(f"Unable to connect to server. Error: {err}")
        return 'Bad Gateway', 502


app = connexion.FlaskApp(__name__, specification_dir='openapi/')
app.add_api('openapi.yml', strict_validation=True, validate_responses=True)


if __name__ == '__main__':
    app.run(port=8080, debug=True)