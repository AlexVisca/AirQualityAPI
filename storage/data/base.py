import mysql.connector
from contextlib import contextmanager
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

@contextmanager
def connect(**kwargs):
    connection = mysql.connector.connect(**kwargs)
    try:
        yield connection

    finally:
        connection.close()


create_temp = '''
    CREATE TABLE IF NOT EXISTS temperature
    (id_ INT NOT NULL AUTO_INCREMENT,
    date_created VARCHAR(100) NOT NULL,
    device_id VARCHAR(250) NOT NULL,
    location VARCHAR(250) NOT NULL,
    temperature DECIMAL(5,2) NOT NULL,
    timestamp VARCHAR(100) NOT NULL,
    trace_id VARCHAR(250) NOT NULL,
    CONSTRAINT temperature_pk PRIMARY KEY (id_))
    '''

create_envr = '''
    CREATE TABLE IF NOT EXISTS environment
    (id_ INT NOT NULL AUTO_INCREMENT,
    date_created VARCHAR(100) NOT NULL,
    device_id VARCHAR(250) NOT NULL,
    location VARCHAR(250) NOT NULL,
    pm2_5 INTEGER NOT NULL,
    co_2 INTEGER NOT NULL,
    timestamp VARCHAR(100) NOT NULL,
    trace_id VARCHAR(250) NOT NULL,
    CONSTRAINT environment_pk PRIMARY KEY (id_))
    '''

empty_temp = '''
    TRUNCATE TABLE temperature;
    '''

empty_envr = '''
    TRUNCATE TABLE environment;
    '''

drop_all = '''
    DROP TABLE temperature, environment;
    '''

version = '''
    SHOW VARIABLES like 'version';
    '''
