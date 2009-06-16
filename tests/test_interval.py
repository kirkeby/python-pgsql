from prelude import assert_eq
from datetime import timedelta
from pgsql import interval

def test_interval_to_timedelta():
    assert_eq(interval(days=1).to_timedelta(),
              timedelta(days=1))

    assert_eq(interval(seconds=1).to_timedelta(),
              timedelta(seconds=1))

    assert_eq(interval(days=1, minutes=3, seconds=1).to_timedelta(),
              timedelta(days=1, seconds=181))

    try:
        interval(years=42).to_timedelta()
        raise AssertionError('Should fail converting year interval.')
    except ValueError:
        pass

def test_interval_eq():
    assert interval(seconds=42) == interval(seconds=42)
    assert interval(seconds=42) <> interval(days=42, seconds=42)
    assert interval(seconds=42) <> interval(days=42)

    assert not interval(seconds=42) == 42
    assert interval(seconds=42) <> 42
    assert interval(days=42) <> 42
    assert interval(years=42) <> 42

def test_interval_repr():
    assert_eq(repr(interval()),
              'interval()')
    assert_eq(repr(interval(seconds=42)),
              'interval(seconds=42)')
    assert_eq(repr(interval(years=7, seconds=42)),
              'interval(years=7, seconds=42)')
