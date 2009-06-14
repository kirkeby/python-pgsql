def roundtrip_value(cu, table, expected):
    cu.execute('DELETE FROM %s' % table)
    cu.execute('INSERT INTO %s(a) VALUES($1)' % table, [expected])
    result, = cu.execute('SELECT * FROM %s' % table).fetchone()
    assert result == expected, `result, expected`
    return result
