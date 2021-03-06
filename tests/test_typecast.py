from pgsql import typecast_date, typecast_time, typecast_datetime, \
                  typecast_interval, typecast_numeric, interval
from datetime import date, time, datetime, timedelta
from prelude import assert_eq

def test_date():
    assert typecast_date(None, '1979-07-07') == date(1979, 07, 07)

def test_datetime():
    assert typecast_datetime(None, '1979-07-07 22:00:12') \
           == datetime(1979, 7, 7, 22, 00, 12)
    assert typecast_datetime(None, '1956-07-09 12:34:45.67891') \
           == datetime(1956, 7, 9, 12, 34, 45, 678910)

def test_time():
    assert typecast_time(None, '12:34:45') == time(12, 34, 45)
    assert typecast_time(None, '12:34:45.67891') == time(12, 34, 45, 678910)

def test_interval():
    assert_eq(typecast_interval(None, '1 day'),
              interval(days=1))

    assert_eq(typecast_interval(None, '12:34:56'),
              interval(hours=12, minutes=34, seconds=56))
    assert_eq(typecast_interval(None, '12:34:56.789'),
              interval(hours=12, minutes=34, seconds=56, microseconds=789000))

    assert_eq(typecast_interval(None, '123 days 12:34:56'),
              interval(days=123, hours=12, minutes=34, seconds=56))

    assert_eq(typecast_interval(None, '1 year -3 days'),
              interval(years=1, days=-3))
