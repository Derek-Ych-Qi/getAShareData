import tushare as ts
import datetime

oneDay = datetime.timedelta(days=1)
tradeCal = ts.util.dateu.trade_cal().set_index('calendarDate')

def dateRange(sdate, edate, step=oneDay, inclusive=True, weekdays=True, *args, **kwargs):
    curDate = sdate
    if inclusive: edate += step
    while curDate < edate:
        if weekdays and curDate.weekday() > 4:
            curDate += step
            continue
        yield curDate
        curDate += step

def tradingDateRange(sdate, edate, step=oneDay, inclusive=True, weekdays=True, *args, **kwargs):
    curDate = sdate
    if inclusive: edate += step
    while curDate < edate:
        if tradeCal.loc[str(curDate), 'isOpen']: yield curDate
        curDate += step
