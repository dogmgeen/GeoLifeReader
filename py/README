  Modifications are needed to the current state of the script in order to
reduce the memory footprint. Below is how this preprocessing script will
be adapted.

Preprocessing Raw GeoLife: raw2db.py
=========================

Input: Raw GeoLife files
Output: Database consisting of modified records.
  Each date is changed to preserve the weekday (e.g. Monday) but all during
  the same week (e.g. starting on Monday, May 20, 2013)


Time Homogeneity output to Database: time_homogenize.py
===================================

Input: Database consisting of modified records.
Output: Database with time-homogenized records.

  users = get_users_present_on(weekday)
  users_present = []
  homogenized_records = []
  for t in range(start, end+delta, delta):
    records = SELECT * FROM records
              WHERE datetime in (t, t+delta)
    for r in records:
      users_present.append(r.user)
      r = modify_record_timestamp(r, t)
      homogenized_records.append(r)

    users_not_present = users - users_present
    for u in users_not_present:
      most_recent_record = most_recent_user_record_before(t, u)
      r = modify_record_timestamp(most_recent_record, t)
      homogenized_records.append(r)

    insert_into_time_homogenized_database(homogenized_records)

    del users_present[:]
    del homogenized_records[:]
      

ONE conversion: db2one.py
==============

Input: Database with time-homogenized records.
Output: ONE simulator ready files.
Parameters: Number of users to output.
            Weekday of interest.
            Area of interest.
            Time delta between records.

  users = get_user_subset(n, weekday)
  for t in range(start, end+delta, delta):
    records = SELECT * FROM time_homogenized_records
              WHERE datetime = t,
                    user in users,
                    point in area_of_interest;
    write_to_file(records)
