from sqlalchemy import Column, Integer, Numeric, String, DateTime
from data.base import Base
from datetime import datetime


class Temperature(Base):
    __tablename__ = "temperature"

    id_ = Column(Integer, primary_key=True)
    date_created = Column(DateTime, nullable=False)
    device_id = Column(String(250), nullable=False)
    location = Column(String(250), nullable=False)
    temperature = Column(Numeric(5,2), nullable=False)
    timestamp = Column(String(100), nullable=False)
    trace_id = Column(String(250), nullable=False)

    def __init__(self, device_id, location, temperature, timestamp, trace_id) -> None:
        self.date_created = datetime.now()
        self.device_id = device_id
        self.location = location
        self.temperature = temperature
        self.timestamp = timestamp
        self.trace_id = trace_id
    
    def to_dict(self):
        dict = {}
        dict['id'] = self.id_
        dict['date_created'] = self.date_created
        dict['device_id'] = self.device_id
        dict['location'] = self.location
        dict['temperature'] = self.temperature
        dict['timestamp'] = self.timestamp
        dict['trace_id'] = self.trace_id

        return dict


class Environment(Base):
    __tablename__ = "environment"

    id_ = Column(Integer, primary_key=True)
    date_created = Column(DateTime, nullable=False)
    device_id = Column(String(250), nullable=False)
    pm2_5 = Column(Integer, nullable=False)
    co_2 = Column(Integer, nullable=False)
    location = Column(String(250), nullable=False)
    timestamp = Column(String(100), nullable=False)
    trace_id = Column(String(250), nullable=False)

    def __init__(self, device_id, pm2_5, co_2, location, timestamp, trace_id) -> None:
        self.date_created = datetime.now()
        self.device_id = device_id
        self.pm2_5 = pm2_5
        self.co_2 = co_2
        self.location = location
        self.timestamp = timestamp
        self.trace_id = trace_id
    
    def to_dict(self):
        dict = {}
        dict['id'] = self.id_
        dict['date_created'] = self.date_created
        dict['device_id'] = self.device_id
        dict['environment'] = {}
        dict['environment']['pm2_5'] = self.pm2_5
        dict['environment']['co_2'] = self.co_2
        dict['location'] = self.location
        dict['timestamp'] = self.timestamp
        dict['trace_id'] = self.trace_id

        return dict