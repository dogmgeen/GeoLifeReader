import geolife
if __name__ == "__main__":
  sample_dir = "/home/djmvfb/Development/GeoLifeReader/"
  geolife_root_directory = geolife.find_geolife_root(sample_dir)
  print("GeoLife root found at {0}".format(geolife_root_directory))

  g = geolife.GeoLifeDataset(geolife_root_directory)
  """
  g.retrieveByDate(date="")\
   .boundByLocation(top="", right="", bottom="", left="")\
   .homogenizeTimeDeltas(time_delta="")\
   .convertToONE(to_file="")\
  """

  print("#"*80)
  print("DATABASE IS BEING DELETED.")
  print("BETTER DELETE THIS LINE BEFORE RUNNING THIS ON THE DATA FOR FIVE HOURS.")
  print("#"*80)
  import os
  os.remove("05d6c855fdfce881d0ddba777c3fcfcd.db")
