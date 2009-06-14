def try_cursor(cursor):
    cursor.execute('SELECT 42')
    assert cursor.fetchone() == (42,)
    assert cursor.fetchone() == None
    cursor.execute('SELECT 42')
    assert cursor.fetchmany() == [(42,)]
    assert cursor.fetchmany() == []
    cursor.execute('SELECT 42')
    assert cursor.fetchall() == [(42,)]
    cursor.close()

def test_cursors():
    try_cursor(cnx.cursor())
    try_cursor(cnx.itercursor())
