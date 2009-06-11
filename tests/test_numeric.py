import sys
from prelude import roundtrip_value
from decimal import Decimal

create_statements = [
    'CREATE TEMPORARY TABLE x(a real)',
    'CREATE TEMPORARY TABLE y(a decimal(2,1))',
    'CREATE TEMPORARY TABLE z(a integer)',
    'CREATE TEMPORARY TABLE w(a bigint)',
]

def test_integer():
    assert isinstance(roundtrip_value(cu, 'z', 42), int)
    assert isinstance(roundtrip_value(cu, 'z', 42L), int)

def test_bigint():
    assert isinstance(roundtrip_value(cu, 'w', 42), long)
    assert isinstance(roundtrip_value(cu, 'w', 42L), long)
    assert isinstance(roundtrip_value(cu, 'w', sys.maxint+1), long)

def test_real():
    assert isinstance(roundtrip_value(cu, 'x', 1.5), float)

def test_decimal():
    assert isinstance(roundtrip_value(cu, 'y', Decimal('1.5')), Decimal)

def test_float_into_decimal():
    cu.execute('INSERT INTO y VALUES($1)', [1.5])
    value, = cu.execute('SELECT a FROM y').fetchone()
    assert isinstance(value, Decimal)
    assert value == Decimal('1.5')
