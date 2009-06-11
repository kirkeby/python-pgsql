from prelude import roundtrip_value

create_statements = [
    'CREATE TEMPORARY TABLE x(a varchar)',
]

def test_utf8():
    cnx.encoding = 'utf-8'
    roundtrip_value(cu, 'x', 'abc')
    roundtrip_value(cu, 'x', u'\xef')

def test_latin1():
    cnx.encoding = 'latin-1'
    roundtrip_value(cu, 'x', 'abc')
    roundtrip_value(cu, 'x', u'\xef')
