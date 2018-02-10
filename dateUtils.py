import datetime

def dateRange(sdate, edate, step=datetime.timedelta(days=1), inclusive=True, weekdays=True, *args, **kwargs):
    curDate = sdate
    if inclusive: edate += step
    while curDate < edate:
        if weekdays and curDate.weekday() > 4:
            curDate += step
            continue
        yield curDate
        curDate += step