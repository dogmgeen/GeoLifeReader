import sys
import os 

"""
g = GeoLifeDataset(directory="/path/containing/raw/plt/files")
g.retrieveByDate(date="")\
 .boundByLocation(top="", right="", bottom="", left="")\
 .homogenizeTimeDeltas(time_delta="")\
 .convertToONE(to_file="")\
"""

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
    from sqlalchemy import create_engine
    engine = create_engine(
      'sqlite:///{0}.db'.format(directory_hash), echo=True
    )

    from sqlalchemy.orm import sessionmaker
    Session = sessionmaker()
    Session.configure(bind=engine)
    session = Session()

    from sqlalchemy.engine.reflection import Inspector
    inspector = Inspector.from_engine(engine)
    import record
    if not record.TABLE_NAME in inspector.get_table_names():
      print("'{0}' does not exist in {1}.db".format(
        record.TABLE_NAME, directory_hash
      ))
      print("'{0}' will be created and populated from files in {1}".format(
        record.TABLE_NAME, directory
      ))
      record.initialize_table(engine)
      record.load_from_directory(directory, session)

    return session


if __name__ == "__main__":
  directory_to_search = sys.argv[1]
  geolife_root_directory = find_geolife_root(directory_to_search)
  print(geolife_root_directory)
