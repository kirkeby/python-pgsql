create_statements = [
    'CREATE TEMPORARY TABLE x(a timestamp without time zone)',
]

def roundtrip_stamp(stamp):
    cnx.execute('INSERT INTO x(a) VALUES($1)', [stamp])
    value, = cu.execute('SELECT * FROM x').fetchone()
    assert value == stamp, `value`

def test_null():
    roundtrip_stamp(None)

def test_Timestamp():
    roundtrip_stamp(dbapi.Timestamp(1979, 7, 7, 22, 00, 00))

def test_precise_Timestamp():
    roundtrip_stamp(dbapi.Timestamp(1979, 7, 7, 22, 00, 12.34))

def test_Date():
    cnx.execute('ALTER TABLE x ALTER COLUMN a TYPE date')
    roundtrip_stamp(dbapi.Date(1979, 7, 7))
