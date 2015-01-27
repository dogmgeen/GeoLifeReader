import sys
import os 

def find_geolife_root(directory_to_search):
  directory_containing_plt = None

  # Walk down tree until a PLT file is encountered.
  for d, subd, files in os.walk(directory_to_search):
    for f in files:
      if f.lower().endswith(".plt"):
        directory_containing_plt = d
        break

  # Return the "Data" directory, which contains all users
  #  and subsequently all raw data files.
  return os.path.dirname(os.path.dirname(directory_containing_plt))


class GeoLifeDataset:
  def __init__(self, directory):
    """Perform a lazy load of the raw PLT files that are located somewhere
    in the provided directory. Code has been written to automatically locate
    these files, so the exact directory is not necessary. All that is required
    is that the raw PLT files be SOMEWHERE within the tree rooted at that
    directory.

    An external database will be populated with the data associated with this
    dataset. If this database has already been created, the database will
    be loaded instead of reading in the raw files based on the hash associated
    with the provided directory."""
    self.db_session = self.__load_db(directory)

  def __load_db(self, directory):
    """Load a database session corresponding to the data within the provided
    directory. This session may need to be created now if it was not created
    prior."""
    # The pre-existing SQLite database will be named according to the
    #  hash created by the directory.
    import hashlib
    directory_hash = hashlib.md5(directory).hexdigest()

    db_name = "{0}.db".format(directory_hash)
    db_exists = os.path.isfile(db_name)

    from sqlalchemy import create_engine
    engine = create_engine(
      'sqlite:///{0}'.format(db_name), echo=True
    )

    from sqlalchemy.orm import sessionmaker
    Session = sessionmaker()
    Session.configure(bind=engine)
    session = Session()

    if not db_exists:
      print("-"*50)
      print("Database does not pre-exist at {0}!".format(db_name))
      print("Database will be created and populated from files in {0}".format(
        directory
      ))
      import record
      record.initialize_table(engine)
      
      for r in load_from_directory(directory):
        print(r)
      print("-"*50)
      #session.add_all()

    return session


import user
def load_from_directory(directory):
  for u in user.from_directory(directory):
    print("Beginning yielding of records from user {0.id}".format(u))
    for record in u:
      yield record

