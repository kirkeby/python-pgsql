from prelude import assert_eq, SkipTest
import pgsql

create_statements = [
    'CREATE TEMPORARY TABLE x(i integer)',
]

def test_module():
    assert callable(pgsql.connect)
    assert_eq(pgsql.apilevel, '2.0')
    assert_eq(pgsql.threadsafety, 1)
    assert_eq(pgsql.paramstyle, 'format')

exceptions = [
    ('StandardError', 'Error'),
    ('StandardError', 'Warning'),
    ('Error', 'InterfaceError'),
    ('Error', 'DatabaseError'),
    ('DatabaseError', 'DataError'),
    ('DatabaseError', 'OperationalError'),
    ('DatabaseError', 'IntegrityError'),
    ('DatabaseError', 'InternalError'),
    ('DatabaseError', 'ProgrammingError'),
    ('DatabaseError', 'NotSupportedError'),
]
def test_error_classes():
    names = pgsql.__dict__.copy()
    names['StandardError'] = StandardError
    for super, sub in exceptions:
        assert names.has_key(sub), 'Mdoule missing %s' % sub
        assert issubclass(names.get(sub), names.get(super)), \
               '%s not a sublass of %s' % (sub, super)
    assert not isinstance(pgsql.Warning, pgsql.Error)

def test_connection():
    assert callable(cnx.close)
    assert callable(cnx.cursor)
    assert callable(cnx.commit)
    assert callable(cnx.rollback)

# FIXME - test the type-snafus.

def test_cursor_description():
    assert cu.description is None

    cu.execute('INSERT INTO x(i) VALUES(%s)', [42,])
    assert cu.description is None

    cu.execute('SELECT * FROM x')
    assert len(cu.description) == 1
    name, type_code, display_size, internal_size, precision, scale, null_ok \
        = cu.description[0]
    assert_eq(name, 'i')
    assert_eq(type_code, pgsql.NUMBER)
    assert null_ok is None or null_ok

    cu.execute('INSERT INTO x(i) VALUES(%s)', [42,])
    assert cu.description is None

def test_cursor_rowcount():
    assert cu.rowcount is -1

    cu.execute('SELECT * FROM x')
    assert cu.rowcount is 0

    cu.execute('INSERT INTO x(i) VALUES(%s)', [42,])
    assert cu.rowcount is 1

    cu.execute('SELECT * FROM x')
    assert cu.rowcount is 1

    cu.execute('UPDATE x SET i=32 WHERE i=21')
    assert cu.rowcount is 0
    cu.execute('UPDATE x SET i=32')
    assert cu.rowcount is 1

    cu.execute('DELETE FROM x WHERE i=21')
    assert cu.rowcount is 0
    cu.execute('DELETE FROM x')
    assert cu.rowcount is 1

def test_cursor_rwonumber():
    raise SkipTest

def test_cursor_arraysize():
    assert_eq(cu.arraysize, 1)

def test_cursor_methods():
    assert callable(cu.setinputsizes)
    assert callable(cu.setoutputsize)
    assert callable(cu.next)
    assert callable(cu.__iter__)
