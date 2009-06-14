def try_cursor(cursor, sql):
    cursor.execute(sql)
    assert cursor.fetchone() == (42,)
    assert cursor.fetchone() == None
    cursor.execute(sql)
    assert cursor.fetchmany() == [(42,)]
    assert cursor.fetchmany() == []
    cursor.execute(sql)
    assert cursor.fetchall() == [(42,)]
    cursor.close()

def test_cursor():
    try_cursor(cnx.cursor(), 'SELECT 42')

def test_itercursor():
    try_cursor(cnx.itercursor(), 'SELECT 42')

def test_prepared():
    try_cursor(cnx.prepare('SELECT 42'), [])
