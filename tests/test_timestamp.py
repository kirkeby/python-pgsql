create_statements = [
    'CREATE TEMPORARY TABLE x(a timestamp without time zone)',
    'CREATE TEMPORARY TABLE y(a date)',
    'CREATE TEMPORARY TABLE z(a interval)',
]

def roundtrip_stamp(table, stamp):
    cnx.execute('INSERT INTO %s(a) VALUES($1)' % table, [stamp])
    value, = cu.execute('SELECT * FROM %s' % table).fetchone()
    assert value == stamp, `value`

def test_null():
    roundtrip_stamp('x', None)
    roundtrip_stamp('y', None)

def test_Timestamp():
    roundtrip_stamp('x', dbapi.Timestamp(1979, 7, 7, 22, 00, 00))

def test_precise_Timestamp():
    roundtrip_stamp('x', dbapi.Timestamp(1979, 7, 7, 22, 00, 12.34))

def test_Date():
    roundtrip_stamp('y', dbapi.Date(1979, 7, 7))

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
    roundtrip_stamp('z', value)

def test_interval():
    value, = cu.execute("SELECT now()-'1979-07-07'").fetchone()
    assert value.days > 1
    value, = cu.execute("SELECT now()-'today'").fetchone()
    assert value.days == 0
    assert value.seconds > 0
