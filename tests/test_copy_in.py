from prelude import assert_eq

create_statements = [
    'CREATE TEMPORARY TABLE x(i integer)',
    'CREATE TEMPORARY TABLE y(a varchar)',
]

def test_copy_in_zero():
    assert_eq(cnx.copy_in('copy x from stdin csv', []),
              0)
    assert_eq(cnx.execute('select * from x').fetchall(),
              [])

def test_copy_in_one():
    assert_eq(cnx.copy_in('copy x from stdin csv', ['42\n']),
              1)
    assert_eq(cnx.execute('select * from x').fetchall(),
              [(42,)])

def test_copy_in_unicode():
    # Try to insert one row with danish ae ligature.
    assert_eq(cnx.copy_in('copy y from stdin csv', ['\xc3\xa6\n']),
              1)
    assert_eq(cnx.execute('select * from y').fetchall(),
              [(u'\xe6',)])

def test_copy_in_many():
    assert_eq(cnx.copy_in('copy x from stdin csv', ['42\n21\n117\n']),
              3)
    assert_eq(cnx.execute('select * from x').fetchall(),
              [(42,), (21,), (117,)])

