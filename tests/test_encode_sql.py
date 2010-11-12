from __future__ import with_statement

import warnings
from pgsql import encode_sql
from prelude import assert_eq, hook, SkipTest

def test_encode_sql():
    assert_eq(encode_sql('SELECT blah FROM foo'),
              'SELECT blah FROM foo')

def test_encode_placeholders():
    assert_eq(encode_sql('SELECT blah FROM foo WHERE blah>%s'),
              'SELECT blah FROM foo WHERE blah>$1')

    assert_eq(encode_sql('SELECT blah FROM foo WHERE blah BETWEEN %s AND %s'),
              'SELECT blah FROM foo WHERE blah BETWEEN $1 AND $2')

    assert_eq(encode_sql('%s %s %s'), '$1 $2 $3')
    assert_eq(encode_sql('%%s %s %%'), '%s $1 %')

def test_unescaped_percentage_warning():
    for string in ['%', '%e']:
        warned = {}
        def warn(message, category=None, stacklevel=1):
            warned[message] = True

        with hook(warnings, 'warn', warn):
            assert_eq(encode_sql(string), string)
            assert warned.get('Unescaped % in SQL')

def test_psql_style_params_warning():
    warned = {}
    def warn(message, category=None, stacklevel=1):
        warned[message] = True

    with hook(warnings, 'warn', warn):
        assert_eq(encode_sql('SELECT $1'), 'SELECT $1')
        assert warned.get('PostgreSQL-style bind-parameters deprecated')

def test_encode_literal():
    raise SkipTest
    assert_eq(encode_sql("SELECT '%s'"),
              "SELECT '%s'")
