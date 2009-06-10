import pgsql

def setup_function():
    global cnx, cu

    cnx = pgsql.connect()
    cu = cnx.cursor()

    cnx.execute('CREATE TEMPORARY TABLE x(a timestamp without time zone)')

def roundtrip_stamp(stamp):
    cnx.execute('INSERT INTO x(a) VALUES($1)', [stamp])
    value, = cu.execute('SELECT * FROM x').fetchone()
    assert value == stamp, `value`

def test_null():
    roundtrip_stamp(None)

def test_Timestamp():
    roundtrip_stamp(pgsql.Timestamp(1979, 7, 7, 22, 00, 00))

def test_precise_Timestamp():
    roundtrip_stamp(pgsql.Timestamp(1979, 7, 7, 22, 00, 12.34))

def test_Date():
    cnx.execute('ALTER TABLE x ALTER COLUMN a TYPE date')
    roundtrip_stamp(pgsql.Date(1979, 7, 7))

def _test_cast_unicode():
    cnx.execute('INSERT INTO x(a) VALUES($1::timestamp)',
                [u'1979-07-07 22:00'])
    rows = cu.execute('SELECT * FROM x').fetchall()
    assert rows == [(u'1979-07-07 22:00:00',)], `rows`

def _test_cast_now():
    cnx.execute('INSERT INTO x(a) VALUES($1::timestamp)', [u'now()'])
    rows = cu.execute('SELECT * FROM x').fetchall()
    (value,), = rows
    assert value.startswith('2009'), `value`

for name, value in globals().items():
    if name.startswith('test_'):
        value.setup = setup_function
