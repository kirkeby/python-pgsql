import pgsql

def test_bytea():
    cnx = pgsql.connect()
    cu = cnx.cursor()

    cnx.execute('CREATE TEMPORARY TABLE bin(a bytea)')

    cnx.execute('INSERT INTO bin(a) VALUES($1)', [pgsql.Binary('\xf8')])
    rows = cu.execute('SELECT * FROM bin').fetchall()
    assert rows == [('\xf8',)], `rows`

