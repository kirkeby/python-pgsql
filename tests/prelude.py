def roundtrip_value(cu, table, stamp):
    cu.execute('DELETE FROM %s' % table)
    cu.execute('INSERT INTO %s(a) VALUES($1)' % table, [stamp])
    value, = cu.execute('SELECT * FROM %s' % table).fetchone()
    assert value == stamp, `value, stamp`
    return value
