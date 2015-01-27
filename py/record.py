from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Float
from sqlalchemy import DateTime
from sqlalchemy import Sequence


DATETIME_STR_FORMAT = "%A, %d. %B %Y %I:%M:%S%p"


Base = declarative_base()
class GeoLifeRecord(Base):
  __tablename__ = "records"
  id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
  user = Column(Integer, index=True)
  latitude = Column(Float)
  longitude = Column(Float)
  datetime = Column(DateTime, index=True)

  def __repr__(self):
    return "<GeoLifeRecord(name={0}, (x,y)=({1}, {2}), datetime={3})>".format(
      self.user, self.latitude, self.longitude,
      self.datetime.strftime(DATETIME_STR_FORMAT)
    )


def initialize_table(engine):
  Base.metadata.create_all(engine)


