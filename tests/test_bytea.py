create_statements = [
    'CREATE TEMPORARY TABLE bin(a bytea)',
]
def test_bytea():
    cnx.execute('INSERT INTO bin(a) VALUES($1)', [dbapi.Binary('\xf8')])
    value, = cu.execute('SELECT * FROM bin').fetchone()
    assert value == '\xf8', `value`
