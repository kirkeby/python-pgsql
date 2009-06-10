create_statements = [
    'CREATE TEMPORARY TABLE bin(a bytea)',
    'CREATE TEMPORARY TABLE txt(a text)',
    'CREATE TEMPORARY TABLE i(a integer)',
    'CREATE TEMPORARY TABLE n(a numeric)',
]

def test_binary():
    cu.execute('SELECT $1', [dbapi.Binary('abc')])
    assert cu.description[0][1] == dbapi.BINARY

    cu.execute('SELECT $1', ['abc'])
    assert cu.description[0][1] == dbapi.BINARY

    cu.execute('SELECT a FROM bin')
    assert cu.description[0][1] == dbapi.BINARY

def test_unicode():
    cu.execute('SELECT $1', [u'abc'])
    assert cu.description[0][1] == dbapi.STRING

    cu.execute('SELECT a FROM txt')
    assert cu.description[0][1] == dbapi.STRING

def test_number():
    cu.execute('SELECT 42')
    assert cu.description[0][1] == dbapi.NUMBER

    cu.execute('SELECT $1', [42])
    assert cu.description[0][1] == dbapi.NUMBER

    cu.execute('SELECT a FROM i')
    assert cu.description[0][1] == dbapi.NUMBER

    cu.execute('SELECT a FROM n')
    assert cu.description[0][1] == dbapi.NUMBER

