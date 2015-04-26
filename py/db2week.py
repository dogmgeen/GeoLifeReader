import config
from raw.record import RawRecord
from raw.record import GeoLifeUser as RawUser
from raw.record import WeekSynthesizedUser

engine = config.getEngine()
Session = sessionmaker()
Session.configure(bind=engine)

def getMinDate(user):
  session = Session()
  result = session.query(RawRecord.date).filter(
    RawRecord.user == user,
  ).order_by(
    RawRecord.date
  ).first()
  logger.info("Earliest date: {0}".format(result))
  return result


def getPreviousMonday(date_of_interest):
  current_weekday = date_of_interest.weekday()
  previous_monday = date_of_interest - timedelta(days=current_weekday)
  assert (previous_monday.weekday() == 0 and previous_monday <= date_of_interest), "Miscalculation of previous Monday of {0}! Actually {1}".format(current_weekday, previous_monday)
  return previous_monday


def getMaxDate(user):
  session = Session()
  result = session.query(RawRecord.date).filter(
    RawRecord.user == user,
  ).order_by(
    RawRecord.date.desc()
  ).first()
  logger.info("Latest date: {0}".format(result))
  return result


def getNextSunday(date_of_interest):
  # If date_of_interest is Sunday, no change is needed.
  current_weekday = date_of_interest.weekday()

  # If the current weekday is Monday, then its numerical value is 0.
  #  Numerical value for Sunday is 6.
  #  6 - 0 = 6.
  #  Add 6 days to Monday and you get the next Sunday.
  next_sunday = date_of_interest + timedelta(days=6-current_weekday)
  assert (next_sunday.weekday() == 6 and current_weekday <= next_sunday), "Miscalculation of previous Monday of {0}! Actually {1}".format(current_weekday, previous_monday)
  return next_sunday


from sqlalchemy.sql import func
def putWeekAndDayStatistics(user, start, end, unique_id):
  # On the unique user database, calculate some statistics for each newly
  #  synthesized users. The statistics include, for a particular user:
  #    * Total number of records for that user's week of activity
  #    *   "     "    "    "      "   "    "     Monday activity
  #    *   "     "    "    "      "   "    "     Tuesday activity
  #    *   "     "    "    "      "   "    "     Wednesday activity
  #    *   "     "    "    "      "   "    "     Thursday activity
  #    *   "     "    "    "      "   "    "     Friday activity
  #    *   "     "    "    "      "   "    "     Saturday activity
  #    *   "     "    "    "      "   "    "     Sunday activity
  #    * Centroid point of locations (i.e. average of latitude and longitude values)
  #
  #   
  # FROM users group by user;
  session = Session()
  new_user_id = makeUniqueUserID(user, unique_id)
  logger.debug("User {0} within ({1}, {2}) => Synthesized user {3}".format(
    user, start, end, new_user_id
  ))

  averages = session.query(
    func.avg(RawRecords.latitude).label("centroid_lat"),
    func.avg(RawRecords.longitude).label("centroid_long)
  ).filter_by(
    RawRecord.user=user, RawRecord.date >= start, RawRecord.date <= end
  ).first()
  logger.debug("Centroid location: ({centroid_lat}, {centroid_long})".format(**averages))

  record_count = session.query(
    func.count(RawRecords)
  ).filter_by(
    RawRecord.user=user, RawRecord.date >= start, RawRecord.date <= end
  ).scalar()
  logger.debug("Number of records: {0}".format(record_count))

  weekday_counts = []
  for weekday in range(7):
    weekday_count = session.query(
      func.count(RawRecords)
    ).filter_by(
      RawRecord.user=user, RawRecord.date >= start, RawRecord.date <= end,
      RawRecord.weekday=weekday
    ).scalar()
    logger.debug("Day {0} has {1} records".format(weekday, weekday_count))
    weekday_counts.append(weekday_counts)
  """
  session.add(WeekSynthesizedUser(
    id=new_user_id,
    centroid_lat=averages.centroid_lat,
    centroid_long=averages.centroid_long,
    week_count=record_count,
    monday_count=weekday_counts[0],
    tuesday_count=weekday_counts[1],
    wednesday_count=weekday_counts[2],
    thursday_count=weekday_counts[3],
    friday_count=weekday_counts[4],
    saturday_count=weekday_counts[5],
    sunday_count=weekday_counts[6]
  ))
  """
  logger.debug("Record added to database\n" + 80*"-")
 

def makeUniqueUserID(current_user_id, unique_id):
  return int("{0}{1}".format(unique_id, current_user_id))


def alterTimestampsAndReassignTrajectory(user, start, end, unique_id):
  session = Session()
  new_user_id = makeUniqueUserID(user, unique_id)



if __name__ == "__main__":
 session = Session()
 users = session.query(RawUser.id).all()
 session.close()

 for u in users:
  logger.info("Current user: {0}".format(u))
  min_date = getMinDate(u)
  first_monday = getPreviousMonday(min_date)
  max_date = getMaxDate(u)
  last_sunday = getNextSunday(max_date)

  week_number = 1
  current_monday = first_monday
  while current_monday < last_sunday:
    current_sunday = getNextSunday(current_monday)

    # For this user during this week, make a record that details the number of
    #  records throughout the week, and the number of records per day in this
    #  week.
    putWeekAndDayStatistics(
      user=u, start=current_monday, end=current_sunday, unique_id=week_number
    )

    # Modify the trajectory for this current week and this current user. This
    #  trajectory should be owned by a new unique user, and its timestamps
    #  should be during a reference week offset by the day-of-week of the
    #  original timestamp.
    #   e.g. Modified timestamp
    #     Original timestamp := Tuesday, May 20th, 2015, at Noon
    #     Reference timestamp := Monday, August 10th, 1993
    #     Altered timestamp := Tuesday, August 11th, 1993, at Noon
    #
    #   e.g. Changed owner
    #     Current week of user's activity := 5th week
    #     Original user ID: 314
    #     Altered user ID: 5314
    alterTimestampsAndReassignTrajectory(
      user=u, start=current_monday, end=current_sunday, unique_id=week_number
    )

    week_number += 1
    current_monday = current_sunday + timedelta(day=1)
