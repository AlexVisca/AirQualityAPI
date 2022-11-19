from sqlalchemy import Column, Integer, Float, String, DateTime
from base import Base


class Stats(Base):
    __tablename__ = "stats"

    id_ = Column(Integer, primary_key=True)
    count = Column(Integer, nullable=False)
    temp_buffer = Column(Float(2), nullable=False)
    max_temp = Column(Float(2), nullable=False)
    min_temp = Column(Float(2), nullable=False)
    avg_temp = Column(Float(2), nullable=False)
    max_pm2_5 = Column(Integer, nullable=False)
    max_co_2 = Column(Integer, nullable=False)
    last_updated = Column(DateTime, nullable=False)

    def __init__(self, count, temp_buffer, max_temp, min_temp, avg_temp, max_pm2_5, max_co_2, last_updated) -> None:
        self.count = count
        self.temp_buffer = temp_buffer
        self.max_temp = max_temp
        self.min_temp = min_temp
        self.avg_temp = avg_temp
        self.max_pm2_5 = max_pm2_5
        self.max_co_2 = max_co_2
        self.last_updated = last_updated
    
    def to_dict(self):
        dict = {}
        dict['count'] = self.count
        dict['temp_buffer'] = self.temp_buffer
        dict['max_temp'] = self.max_temp
        dict['min_temp'] = self.min_temp
        dict['avg_temp'] = self.avg_temp
        dict['max_pm2_5'] = self.max_pm2_5
        dict['max_co_2'] = self.max_co_2
        dict['last_updated'] = self.last_updated.strftime("%Y-%m-%dT%H:%M:%S")

        return dict
