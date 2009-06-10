import pgsql

def test_text():
    cnx = pgsql.connect()
    cu = cnx.cursor()

    cnx.execute('CREATE TEMPORARY TABLE txt(a text)')

    cnx.execute('INSERT INTO txt(a) VALUES($1)', ['abc'])
    rows = cu.execute('SELECT * FROM txt').fetchall()
    assert rows == [(u'abc',)], `rows`

    cnx.execute('UPDATE txt SET a=$1', [u'\xe6\xf8\xe5'])
    rows = cu.execute('SELECT * FROM txt').fetchall()
    assert rows == [(u'\xe6\xf8\xe5',)], `rows`
