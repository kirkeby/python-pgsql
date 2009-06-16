from prelude import assert_eq, SkipTest

create_statements = [
    'CREATE TEMPORARY TABLE x(i integer)',
]

def try_cursor_fetch(cursor, sql):
    # FIXME - test that fetch* raise an Error when execute* returned no
    # result set, and before execute was called.
    cursor.execute(sql)
    assert cursor.fetchone() == (42,)
    assert cursor.fetchone() == None

    # FIXME - test that fetchmany obeys arraysize
    # FIXME - test that fetchmany obeys size-argument
    cursor.execute(sql)
    assert cursor.fetchmany() == [(42,)]
    assert cursor.fetchmany() == []

    cursor.execute(sql)
    assert cursor.fetchall() == [(42,)]

    cursor.close()

executemany_rows = [(21,), (42,), (117,)]
def assert_executemany():
    assert_eq(sorted(cnx.execute('SELECT * FROM x')),
              executemany_rows)

def try_cursor_executemany(cursor):
    cursor.executemany('INSERT INTO x(i) VALUES(%s)', executemany_rows)
    assert_executemany()

def test_cursor():
    try_cursor_fetch(cnx.cursor(), 'SELECT 42')
    try_cursor_executemany(cnx.cursor())

def test_itercursor():
    try_cursor_fetch(cnx.itercursor(), 'SELECT 42')
    try_cursor_executemany(cnx.itercursor())

def test_prepared():
    raise SkipTest
    try_cursor_fetch(cnx.prepare('SELECT 42'), [])

def test_prepared_executemany():
    raise SkipTest
    c = cnx.prepare('INSERT INTO x(i) VALUES(%s)')
    c.executemany(executemany_rows)
    assert_executeman()
