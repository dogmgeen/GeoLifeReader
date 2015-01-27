from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Float
from sqlalchemy import DateTime
from sqlalchemy import Sequence


DATETIME_STR_FORMAT = "%A, %d. %B %Y %I:%M%p"

Base = declarative_base()
class GeoLifeRecord(Base):
  __tablename__ = 'records'
  id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
  user = Column(Integer, index=True)
  latitude = Column(Float)
  longitude = Column(Float)
  datetime = Column(DateTime, index=True)

  def __repr__(self):
    return "<GeoLifeRecord(name={0}, (x,y)=({1}, {2}), datetime={3}".format(
      self.user, self.latitude, self.longitude,
      self.datetime.strftime(DATETIME_STR_FORMAT)
    )


if __name__ == "__main__":
  from sqlalchemy import create_engine
  engine = create_engine('sqlite:///:memory:', echo=True)
  Base.metadata.create_all(engine)

  from sqlalchemy.orm import sessionmaker
  Session = sessionmaker()
  Session.configure(bind=engine)
  session = Session()

  sample_dir = "/home/kp/Development/GeoLifeReader/"
  import geolife
  geolife_root_directory = geolife.find_geolife_root(sample_dir)
  print("GeoLife root found at {0}".format(geolife_root_directory))

  g = geolife.GeoLifeDataset(geolife_root_directory)

  import os
  print("#"*80)
  print("DATABASE IS BEING DELETED.")
  print("BETTER DELETE THIS LINE BEFORE RUNNING THIS ON THE DATA FOR FIVE HOURS.")
  print("#"*80)
  os.remove("a426bda7c5585e6a903411b89c3d334d.db")
