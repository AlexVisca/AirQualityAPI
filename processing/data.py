import sqlite3
from sqlite3 import OperationalError
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

def query(database, query):
    with sqlite3.connect(database) as conn:
        c = conn.cursor()
        try:
            c.execute(query)
        finally:
            conn.commit()

create = '''
    CREATE TABLE Stats
    (id_ INTEGER PRIMARY KEY ASC,
    count INTEGER NOT NULL,
    temp_buffer FLOAT NOT NULL,
    max_temp FLOAT,
    min_temp FLOAT,
    avg_temp FLOAT,
    max_pm2_5 INTEGER,
    max_co_2 INTEGER,
    last_updated VARCHAR(100) NOT NULL)
'''

drop = '''
    DROP TABLE stats
    '''

empty = '''
    DELETE FROM Stats;
    '''

vacuum = '''
    VACUUM;
    '''

version = '''
    SELECT SQLITE_VERSION() AS Version;
    '''
