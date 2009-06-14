create_statements = [
    'CREATE TEMPORARY TABLE x(a timestamp without time zone)',
    'CREATE TEMPORARY TABLE y(a date)',
    'CREATE TEMPORARY TABLE z(a interval)',
    'CREATE TEMPORARY TABLE w(a time without time zone)',
]

from prelude import roundtrip_value

def test_null():
    roundtrip_value(cu, 'x', None)
    roundtrip_value(cu, 'y', None)

def test_Timestamp():
    roundtrip_value(cu, 'x', dbapi.Timestamp(1979, 7, 7, 22, 00, 00))

def test_precise_Timestamp():
    ts = dbapi.Timestamp(1979, 7, 7, 22, 00, 12.33)
    assert str(ts) == '1979-07-07 22:00:12.330000', str(ts)
    roundtrip_value(cu, 'x', ts)

def test_precise_Time():
    ts = dbapi.Time(22, 00, 12.33)
    assert str(ts) == '22:00:12.330000', str(ts)
    roundtrip_value(cu, 'w', ts)

def test_Date():
    roundtrip_value(cu, 'y', dbapi.Date(1979, 7, 7))

def test_time():
    value, = cu.execute('SELECT now()::time without time zone').fetchone()
    assert value.hour < 24
    value, = cu.execute('SELECT now()::time with time zone').fetchone()
    assert value.hour < 24

def test_timestamp():
    value, = cu.execute('SELECT now()').fetchone()
    assert value.hour < 24
    value, = cu.execute('SELECT now()::timestamp with time zone').fetchone()
    assert value.hour < 24
    value, = cu.execute('SELECT now()::timestamp with time zone').fetchone()
    assert value.hour < 24

def test_Interval():
    value, = cu.execute("SELECT now()-'1979-07-07'").fetchone()
    roundtrip_value(cu, 'z', value)

def test_interval():
    value, = cu.execute("SELECT now()-'1979-07-07'").fetchone()
    assert value.days > 1
    value, = cu.execute("SELECT now()-'today'").fetchone()
    assert value.days == 0
    assert value.seconds > 0

from datetime import time

def test_datetime():
    t = time(22, 00, 42, 100)
    assert str(t) == '22:00:42.000100', str(t)
    assert not hasattr(t, '__quote__')
    assert not hasattr(t, '__pgquote__')
    assert not hasattr(t, '__pgsql_typeoid__')
    assert not hasattr(t, '__binary__')
    assert not callable(t)

