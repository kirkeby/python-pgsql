#!/usr/bin/python
#
# pgsql Simple Python PostgreSQL interface
#
# Copyright (c) 2006-2007 Cristian Gafton <gafton@rpath.com>, rPath, Inc.
#
# this servs as a "show me how it works" demo

# To set it up you need to create a PostgreSQL user 'luser' and a
# corresponding database 'luser'. As postgres / postmaster user, you
# can run:
#       createuser -RDS luser
#       createdb -O=luser luser

# Alternatively, you can change the connect parameters below to something
# that works. Only use a temporary table is used for the demo

# In this demo you can see:
# 1. Working With Regular Cursors
# 2. Working With Prepared Statements
# 3. Working With Executemany
# 4. Working With Blobs
# 5. Working with bulk inserts

import time
import pgsql

#db = pgsql.connect(host = "localhost", user = "luser", database = "luser")
db = pgsql.connect(host = "localhost", database = "junk")

# how many rows we test with
ROWS = 20000

# create the test table
print "Creating test table"
db.execute("""
CREATE TEMPORARY TABLE demo(
    id SERIAL,
    name VARCHAR(100),
    intval INTEGER,
    blobval BYTEA
)""")

#
# WORKING WITH REGULAR CURSORS
#
print "Testing core functionality..."
cu = db.cursor()
for x in range(1):
    # here is how you use bind parameters
    cu.execute("INSERT INTO demo(intval, name) VALUES ($1,$2)",
               (x, str(x*10)) )
# try an op with no parameters
cu.execute("SELECT * FROM demo")
for row in cu:
    # row is a tuple; you can convert it to a dict easily
    print row, "a.k.a", dict(zip(cu.fields, row))

#
# WORKING WITH PREPARED STATEMENTS
#
pcu0 = db.prepare("INSERT INTO demo(intval, name) VALUES ($1,$2)")
for x in range(1):
    pcu0.execute(x, "pcu%d" % x)
cu.execute("SELECT * FROM demo where name like 'pcu%'")
for row in cu:
    print row
print

#
# WORKING WITH EXECUTEMANY
#
print "Timing insert speed..."
query = "INSERT INTO demo(intval, name) VALUES ($1, 'speedtest')"
rows = range(ROWS)
# helper fucntion that computes percentage gains
def pct(t1, t2):
    if t1>=t2:
        return "%.2f%% gain" % ((t1-t2)*100.0/t1,)
    return "%.2f%% loss" % ((t2-t1)*100.0/t1,)

# for loop using standard cursors
tcu1 = time.time()
cu = db.cursor()
for x in rows:
    cu.execute(query, x)
tcu1 = time.time() - tcu1
print "Regular cursor  insert of %d rows by loop-execute: %.2f sec (%s)" % (
    len(rows), tcu1, "baseline")

# executemany using standard cursors
tcu2 = time.time()
cu = db.cursor()
cu.executemany(query, rows)
tcu2 = time.time() - tcu2
print "Regular cursor  insert of %d rows by executemany:  %.2f sec (%s)" % (
    len(rows), tcu2, pct(tcu1, tcu2))

# for loop using prepared statement
tpcu1 = time.time()
pcu = db.prepare(query)
for x in rows:
    pcu.execute(x)
tpcu1 = time.time() - tpcu1
print "Prepared cursor insert of %d rows by loop-execute: %.2f sec (%s)" % (
    len(rows), tpcu1, pct(tcu1,tpcu1))

# executemany using prepared statement
tpcu2 = time.time()
pcu = db.prepare(query)
pcu.executemany(rows)
tpcu2 = time.time() - tpcu2
print "Prepared cursor insert of %d rows by executemany:  %.2f sec (%s)" % (
    len(rows), tpcu2, pct(tcu1,tpcu2))

print

#
# WORKING WITH BLOBS
#
print "Testing BLOBs"
# get a blob from somewhere
blob = open("/bin/bash", "r").read()
# we can use any type of cursor for inserting blobs
cu = db.cursor()
# let's insert this a few times
for x in range(5):
    cu.execute("INSERT INTO demo(name, blobval) VALUES ($1, $2)",
               ("blob%d" % x, blob))
# Now retrieve the blobs - for retrieval of the large datasets we can
# use an iteration cursor, which will only load up one row at a time
# in the client memory
icu = db.itercursor()
# let's use a bind parameter just for kicks
icu.execute("SELECT id, name, blobval FROM demo WHERE name LIKE $1",
            "blob%")
for (id, name, b) in icu:
    print "Checking %s id=%d len=%d" % (name, id, len(b)), b == blob
print

#
# SPEED DIFFERENCE between a standard cursor and an iterator cursor
#
print "Timing standard/iterator cursor speed difference..."
query = "SELECT * FROM demo WHERE BLOBVAL IS NULL"

# first, do a count on the above query to determine how many results to expect
rows = db.execute("SELECT COUNT(*) FROM (%s) AS q" % query).fetchone()[0]

# regular cursor
tcu = time.time()
cu = db.cursor()
cu.execute(query)
for x in cu:
    pass
tcu = time.time() - tcu
print "Standard cursor retrieved %d rows in %.2f sec" % (rows, tcu)

# iterator cursor
ticu = time.time()
icu = db.itercursor()
icu.execute(query)
for x in icu:
    pass
ticu = time.time() - ticu
print "Iterator cursor retrieved %d rows in %.2f sec" % (rows, ticu)

# Working with BULK inserts
#
# bulkload(table, rows, fieldnames)
db.bulkload("demo", [ ("test-varchar", "test-bytea") ],
            ("name", "blobval"))
print
print "Testing bulk load speed for %d rows..." % (ROWS,)
# Speed difference in inserts using bulkload vs inserts
import itertools

# for loop using prepared statement
db.execute("delete from demo")
t1 = time.time()
cu = db.prepare("INSERT INTO demo (name, blobval) values ($1,$2)")
for (c1,c2) in itertools.izip(
    ("name%dvalue" % x for x in xrange(ROWS)),
    ("blobval%dvalue" % x for x in xrange(ROWS))
    ):
    cu.execute(c1, c2)
tinsert = time.time() - t1
print "INSERT PREPARED %d rows: %.2f sec (%s)" % (ROWS, tinsert, "baseline")

# multival insert
db.execute("delete from demo")
cu = db.cursor()
t1 = time.time()
q = "INSERT INTO demo (name, blobval) VALUES "
qvals = ",".join( "('%s','%s')" % (a,b) for a,b in
                  itertools.izip( ("name%dvalue" % x for x in xrange(ROWS)),
                                  ("blobval%dvalue" % x for x in xrange(ROWS)) )
                  )
cu.execute(q+qvals)
tmultival = time.time() - t1
print "INSERT MULTIVAL %d rows: %.2f sec (%s)" % (ROWS, tmultival, pct(tinsert, tmultival))

# bulkload
db.execute("delete from demo")
t1 = time.time()
db.bulkload("demo", itertools.izip(
    ("name%dvalue" % x for x in xrange(ROWS)),
    ("blobval%dvalue" % x for x in xrange(ROWS))
    ), ("name", "blobval"))
tbulk = time.time() - t1
print "INSERT BULKLOAD %d rows: %.2f sec (%s)" % (ROWS, tbulk, pct(tinsert, tbulk))

#
# DEMO DONE
#
db.close()
print "All done"
