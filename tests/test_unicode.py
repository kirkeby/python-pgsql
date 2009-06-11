create_statements = [
    'CREATE TEMPORARY TABLE txt(a text)',
]

from prelude import roundtrip_value

def test_with_str():
    assert isinstance(roundtrip_value(cu, 'txt', 'abc'), unicode)

def test_with_unicode():
    assert isinstance(roundtrip_value(cu, 'txt', u'\xe6\xf8\xe5'), unicode)
