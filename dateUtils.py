import datetime

def dateRange(sdate, edate, step=datetime.timedelta(days=1), inclusive=True, weekdays=True, *args, **kwargs):
    curDate = sdate
    while curDate < edate:
        if weekdays and curDate.weekday() > 4:
            curDate += step
            continue
        yield curDate
        curDate += step
    if inclusive and curDate == edate:
        yield curDate