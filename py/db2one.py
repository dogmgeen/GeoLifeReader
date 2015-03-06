#Input: Database with time-homogenized records.
#Output: ONE simulator ready files.
#Parameters: Number of users to output.
#            Weekday of interest.
#            Area of interest.
#            Time delta between records.
from schema import HomogenizedRecord


if __name__ == "__main__":
  users = get_user_subset(n, weekday)
  for t in timerange(time.min, time.max, delta):
    records = session.query(HomogenizedRecord).filter(
      HomogenizedRecord.time == t,
      HomogenizedRecord.user.in_(users),
    )
SELECT * FROM time_homogenized_records
              WHERE datetime = t,
                    user in users,
                    point in area_of_interest;
    write_to_file(records)
