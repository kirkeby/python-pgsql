from contextlib import contextmanager

def assert_eq(actual, expected):
    assert actual == expected, '%r is not %r' % (actual, expected)

def roundtrip_value(cu, table, expected):
    cu.execute('DELETE FROM %s' % table)
    cu.execute('INSERT INTO %s(a) VALUES(%%s)' % table, [expected])
    result, = cu.execute('SELECT * FROM %s' % table).fetchone()
    assert result == expected, `result, expected`
    return result

@contextmanager
def hook(namespace, name, value, getter=getattr, setter=setattr):
    old_value = getter(namespace, name)
    setter(namespace, name, value)
    try:
        yield
    finally:
        setter(namespace, name, old_value)
