from datetime import datetime
import pytz
import calendar


def normalize_date(time_in_seconds):
    dt = datetime.utcfromtimestamp(time_in_seconds)
    normalized_dt = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.utc)
    return int(calendar.timegm(normalized_dt.timetuple()))
