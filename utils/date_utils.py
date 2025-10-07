from utils.db import connect_to_iris
from datetime import datetime, timedelta


def get_min_obs_date():
    conn = connect_to_iris()
    cursor = conn.cursor()
    cursor.execute("SELECT MIN(OBS_Date) FROM SQLUser.MR_Observations")
    result = cursor.fetchone()[0]
    conn.close()
    return result


def generate_month_periods(start_date, end_date):
    periods = []
    current = start_date.replace(day=1)

    while current <= end_date:
        next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
        periods.append((current.strftime('%Y-%m-%d'), (next_month - timedelta(days=1)).strftime('%Y-%m-%d')))
        current = next_month

    return periods

