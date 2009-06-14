from pgsql import typecast_date, typecast_time, typecast_datetime, \
                  typecast_interval, typecast_numeric, interval
from datetime import date, time, datetime

def assert_eq(actual, expected):
    assert actual == expected, '%r is not %r' % (actual, expected)

def test_date():
    assert typecast_date('1979-07-07') == date(1979, 07, 07)

def test_datetime():
    assert typecast_datetime('1979-07-07 22:00:12') \
           == datetime(1979, 7, 7, 22, 00, 12)
    assert typecast_datetime('1956-07-09 12:34:45.67891') \
           == datetime(1956, 7, 9, 12, 34, 45, 678910)

def test_time():
    assert typecast_time('12:34:45') == time(12, 34, 45)
    assert typecast_time('12:34:45.67891') == time(12, 34, 45, 678910)

def test_interval():
    assert_eq(typecast_interval('1 day'),
              interval(days=1))

    assert_eq(typecast_interval('12:34:56'),
              interval(hours=12, minutes=34, seconds=56))
    assert_eq(typecast_interval('12:34:56.789'),
              interval(hours=12, minutes=34, seconds=56, microseconds=789000))

    assert_eq(typecast_interval('123 days 12:34:56'),
              interval(days=123, hours=12, minutes=34, seconds=56))

    assert_eq(typecast_interval('1 year -3 days'),
              interval(years=1, days=-3))
