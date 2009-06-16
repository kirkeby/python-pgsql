#!/usr/bin/env python
import dbapi20
import unittest
import pgsql
import popen2

class test_pgsql(dbapi20.DatabaseAPI20Test):
    driver = pgsql
    connect_args = ()
    connect_kw_args = {}

    lower_func = 'lower' # For stored procedure test

    def setUp(self):
        # Call superclass setUp In case this does something in the
        # future
        dbapi20.DatabaseAPI20Test.setUp(self)

    def tearDown(self):
        dbapi20.DatabaseAPI20Test.tearDown(self)

    def test_nextset(self):
        pass
    def test_setoutputsize(self):
        pass
    def test_ExceptionsAsConnectionAttributes(self):
        pass

if __name__ == '__main__':
    unittest.main()
