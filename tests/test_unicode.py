create_statements = [
    'CREATE TEMPORARY TABLE txt(a text)',
]

def test_with_str():
    cnx.execute('INSERT INTO txt(a) VALUES($1)', ['abc'])
    rows = cu.execute('SELECT * FROM txt').fetchall()
    assert rows == [(u'abc',)], `rows`

def test_with_unicode():
    cnx.execute('INSERT INTO txt(a) VALUES($1)', [u'\xe6\xf8\xe5'])
    rows = cu.execute('SELECT * FROM txt').fetchall()
    assert rows == [(u'\xe6\xf8\xe5',)], `rows`
