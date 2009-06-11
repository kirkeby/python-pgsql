from prelude import roundtrip_value

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

date_types = [
    'date',
    'abstime',
    'timestamp without time zone',
    'timestamp with time zone',
    'time with time zone',
    'time without time zone',
]
def test_date_types():
    for date_type in date_types:
        yield check_date_type, date_type
def check_date_type(t):
    cu.execute("SELECT now()::%s" % t)
    assert cu.description[0][1] == dbapi.DATETIME, cu.description[0][1]

number_types = [
    'smallint', 'integer', 'bigint', 'decimal', 'numeric', 'real',
    'double precision',
]
def test_number_types():
    for number_type in number_types:
        yield check_number_type, number_type
def check_number_type(t):
    cu.execute("SELECT 42::%s" % t)
    assert cu.description[0][1] == dbapi.NUMBER, cu.description[0][1]

character_types = [
    'character(3)', 'character varying', 'text',
]
def test_character_types():
    for character_type in character_types:
        yield check_character_type, character_type
def check_character_type(t):
    cu.execute("SELECT '117'::%s" % t)
    assert cu.description[0][1] == dbapi.STRING, cu.description[0][1]

def test_without_string_typecast():
    del cnx.typecasts['string']
    assert isinstance(roundtrip_value(cu, 'txt', 'abc'), str)
