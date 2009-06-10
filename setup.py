#!/usr/bin/env python

"""Setup script for pgsql version 4.0

Authors and history:
* based on code written 1995 by Pascal Andre <andre@chimay.via.ecp.fr>
* PyGreSQL written 1997 by D'Arcy J.M. Cain <darcy@druid.net>
* setup script created 2000/04 Mark Alexander <mwa@gate.net>
* tweaked 2000/05 Jeremy Hylton <jeremy@cnri.reston.va.us>
* win32 support 2001/01 by Gerhard Haering <gerhard@bigfoot.de>
* tweaked 2006/02 Christoph Zwerschke <cito@online.de>
* updated 2006/08 for pgsql by Cristian Gafton <gafton@rpath.com>

Prerequisites to be installed:
* Python including devel package (header files and distutils)
* PostgreSQL libs and devel packages (header files of client and server)
* PostgreSQL pg_config tool (usually included in the devel package)
  (the Windows installer has it as part of the database server feature)

Tested with Python 2.4.3 and PostGreSQL 8.1.4. Older version should work
as well, but you will need at least Python 2.1 and PostgreSQL 8.0.

Use as follows:
python setup.py build   # to build the module
python setup.py install # to install it

For Win32, you should have the Microsoft Visual C++ compiler and
the Microsoft .NET Framework SDK installed and on your search path.
If you want to use the free Microsoft Visual C++ Toolkit 2003 compiler,
you need to patch distutils (www.vrplumber.com/programming/mstoolkit/).
Alternatively, you can use MinGW (www.mingw.org) for building on Win32:
python setup.py build -c mingw32 install # use MinGW
Note that the official Python distribution is now using msvcr71 instead
of msvcrt as its common runtime library. So, if you are using MinGW to build
PyGreSQL, you should edit the file "%MinGWpath%/lib/gcc/%MinGWversion%/specs"
and change the entry that reads -lmsvcrt to -lmsvcr71.

See www.python.org/doc/current/inst/ for more information
on using distutils to install Python programs.
"""

from distutils.core import setup
from distutils.extension import Extension
import sys, os

def pg_config(s):
	"""Retrieve information about installed version of PostgreSQL."""
	f = os.popen("pg_config --%s" % s)
	d = f.readline().strip()
	if f.close() is not None:
		raise Exception, "pg_config tool is not available."
	if not d:
		raise Exception, "Could not get %s information." % s
	return d

def mk_include():
	"""Create a temporary local include directory.

	The directory will contain a copy of the PostgreSQL server header files,
	where all features which are not necessary for PyGreSQL are disabled.
	"""
	os.mkdir('include')
	for f in os.listdir(pg_include_dir_server):
		if not f.endswith('.h'):
			continue
		d = file(os.path.join(pg_include_dir_server, f)).read()
		if f == 'pg_config.h':
			d += '\n'
			d += '#undef ENABLE_NLS\n'
			d += '#undef USE_REPL_SNPRINTF\n'
			d += '#undef USE_SSL\n'
		file(os.path.join('include', f), 'w').write(d)

def rm_include():
	"""Remove the temporary local include directory."""
	if os.path.exists('include'):
		for f in os.listdir('include'):
			os.remove(os.path.join('include', f))
		os.rmdir('include')

pg_include_dir = pg_config('includedir')
pg_include_dir_server = pg_config('includedir-server')

rm_include()
mk_include()

include_dirs = ['include', pg_include_dir,  pg_include_dir_server]

pg_libdir = pg_config('libdir')
library_dirs = [pg_libdir]

libraries=['pq']

if sys.platform == "win32":
	include_dirs.append(os.path.join(pg_include_dir_server, 'port/win32'))

setup(
	name = "python-pgsql",
	version = "0.9.7",
	description = "PostgreSQL bindings for Python w/ bind parameters support",
	author = "Cristian Gafton",
	author_email = "gafton@rpath.com",
	url = "http://people.rpath.com/~gafton",
	license = "Python",
	py_modules = ['pgsql'],
	ext_modules = [Extension(
		'_pgsql', ['pgsql.c'],
		include_dirs = include_dirs,
		library_dirs = library_dirs,
		libraries = libraries,
		extra_compile_args = ['-O2'],
		)],
        long_description = """
This is a set of Python bindings for PostgreSQL derived from the PyGreSQL 3.8.1 sources. The main changes from the PyGreSQL module are:

* support for bind parameters, alleviating the need for extensive, expensive and vulnerable quoting of user-supplied data
* support for server side cursors for looping through large query results without loading up the entire result set in the client's memory
* returned values are translated to Python objects in the C layer, resulting in some speed improvement
* DB API 2.0 compliance, with some extensions


PostgreSQL allows for numeric parameters to be passed in a query (bind parameters). If parameters are used, they are referred to in the query string as $1, $2, etc. Bind parameters are passed in as a tuple that holds the values bound in the query. The primary advantage of using bind parameters is that parameter values may be separated from the command string, thus avoiding the need for tedious and error-prone quoting and escaping.

Because of the different query syntax between what PostgreSQL natively supports and what PyGreSQL used to implement (Python style substitutions like %s and %(name)s), these bindings will not grok SQL queries written for PyGreSQL - you might need to slightly rework some of your queries to take advantage of the bind parameter support.
""",)

rm_include()
