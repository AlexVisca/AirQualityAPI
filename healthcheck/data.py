import sqlite3
from sqlite3 import OperationalError
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Health(Base):
    __tablename__ = "health"

    id_ = Column(Integer, primary_key=True)
    system = Column(String(250), nullable=False)
    receiver = Column(String(250), nullable=False)
    storage = Column(String(250), nullable=False)
    auditlog = Column(String(250), nullable=False)
    processing = Column(String(250), nullable=False)
    last_updated = Column(DateTime, nullable=False)

    def __init__(self, system, receiver, storage, auditlog, processing, last_updated) -> None:
        self.system = system
        self.receiver = receiver
        self.storage = storage
        self.auditlog = auditlog
        self.processing = processing
        self.last_updated = last_updated

    def to_dict(self):
        dict = {}
        dict['system'] = self.system
        dict['receiver'] = self.receiver
        dict['storage'] = self.storage
        dict['auditlog'] = self.auditlog
        dict['processing'] = self.processing
        dict['last_updated'] = self.last_updated

        return dict


def sqlite_client(database, query):
    with sqlite3.connect(database) as conn:
        c = conn.cursor()
        try:
            c.execute(query)
            conn.commit()
        finally:
            c.close()

create_table = '''
    CREATE TABLE IF NOT EXISTS health
    (id_ INTEGER PRIMARY KEY ASC,
    system VARCHAR(250) NOT NULL,
    receiver VARCHAR(250) NOT NULL,
    storage VARCHAR(250) NOT NULL,
    auditlog VARCHAR(250) NOT NULL,
    processing VARCHAR(250) NOT NULL,
    last_updated VARCHAR(100) NOT NULL)
'''

drop_table = '''
    DROP TABLE health
    '''

empty_table = '''
    DELETE FROM health;
    '''

vacuum = '''
    VACUUM;
    '''

version = '''
    SELECT SQLITE_VERSION() AS Version;
    '''
