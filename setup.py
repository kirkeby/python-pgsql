#!/usr/bin/env python

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

pg_include_dir = pg_config('includedir')
pg_include_dir_server = pg_config('includedir-server')
pg_libdir = pg_config('libdir')

include_dirs = [pg_include_dir,  pg_include_dir_server]
library_dirs = [pg_libdir]
libraries=['pq']

if sys.platform == "win32":
    include_dirs.append(os.path.join(pg_include_dir_server, 'port/win32'))

setup(
    # FIXME - Should probably change the package name, since this is no
    # longer a simple fork.
    name = "python-pgsql",
    version = "0.9.7-20091109",
    description = "PostgreSQL bindings for Python w/ bind parameters support",
    author = "Sune Kirkeby",
    author_email = "mig@ibofobi.dk",
    url = "http://github.com/kirkeby/python-pgsql/",
    license = "Python",
    py_modules = ['pgsql'],
    ext_modules = [
        Extension('_pgsql', ['_pgsql.c'],
            include_dirs = include_dirs,
            library_dirs = library_dirs,
            libraries = libraries,
            extra_compile_args = ['-O2'],
        ),
    ],
)
