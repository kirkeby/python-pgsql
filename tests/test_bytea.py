create_statements = [
    'CREATE TEMPORARY TABLE bin(a bytea)',
]
def test_bytea():
    cnx.execute('INSERT INTO bin(a) VALUES(%s)', [dbapi.Binary('\xf8')])
    value, = cu.execute('SELECT * FROM bin').fetchone()
    assert value == '\xf8', `value`
    assert isinstance(value, str)
