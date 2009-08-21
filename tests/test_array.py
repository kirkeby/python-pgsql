def check(sql, expected, etype=None):
    value, = cnx.execute('SELECT ARRAY[%s]' % sql).fetchone()
    assert isinstance(value, list), `type(value)`
    if value and etype:
        assert isinstance(value[0], etype), \
               '%s is not %s' % (type(value[0]), etype)
    assert value == expected, '%r is not %r' % (value, expected)

def test_empty():
    value, = cnx.execute("SELECT '{}'::int[]").fetchone()
    assert isinstance(value, list), `type(value)`
    assert value == [], '%r is not %r' % (value, [])

def test_bool():
    check('true, false', [True, False], bool)

def test_int():
    check('42, 117, 128', [42, 117, 128], int)

def test_str_simple():
    check("'a', 'b'", ['a', 'b'], unicode)

def test_str_quote():
    check("'\"'", ['"'], unicode)

def test_str_backslash():
    check("'\\\\'", ['\\'], unicode)

def test_str_comma():
    check("','", [','], unicode)

def test_str_newline():
    check("'\n'", ['\n'], unicode)
