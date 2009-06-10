/*
 * pgsql, written by Cristian Gafton (c) 2006-2007, rPath, Inc.
 *
 * Based on previous work done by:
 * PyGres, version 2.2 A Python interface for PostgreSQL database. Written by
 * D'Arcy J.M. Cain, (darcy@druid.net).  Based heavily on code written by
 * Pascal Andre, andre@chimay.via.ecp.fr. Copyright (c) 1995, Pascal Andre
 * (andre@via.ecp.fr).
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written
 * agreement is hereby granted, provided that the above copyright notice and
 * this paragraph and the following two paragraphs appear in all copies or in
 * any new file that contains a substantial portion of this file.
 *
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT,
 * SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS,
 * ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF THE
 * AUTHOR HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * THE AUTHOR SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND THE
 * AUTHOR HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
 * ENHANCEMENTS, OR MODIFICATIONS.
 *
 * Further modifications copyright 1997, 1998, 1999 by D'Arcy J.M. Cain
 * (darcy@druid.net) subject to the same terms and conditions as above.
 *
 * pgsql has been reworked from PyGreSQL by
 * Cristian Gafton <gafton@rpath.com>, (c) 2006-2007 for rPath, Inc.
 * subject to the same terms and conditions as above.
 */

/* pg large object */
typedef struct
{
	PyObject_HEAD
	pgobject	*pgcnx;		/* parent connection object */
	Oid		lo_oid;		/* large object oid */
	int		lo_fd;		/* large object fd */
}	pglargeobject;

staticforward PyTypeObject PglargeType;

#define is_pglargeobject(v) ((v)->ob_type == &PglargeType)

/* taken from fileobject.c */
#define BUF(v) PyString_AS_STRING((PyStringObject *)(v))

/* checks large object validity */
static int
check_lo_obj(pglargeobject *self, int level)
{
    if (!check_pg_obj(self->pgcnx))
	return 0;

    if (!self->lo_oid) {
	PyErr_SetString(IntegrityError, "object is not valid (null oid).");
	return 0;
    }

    if (level & CHECK_OPEN) {
	if (self->lo_fd < 0) {
	    PyErr_SetString(PyExc_IOError, "object is not opened.");
	    return 0;
	}
    }

    if (level & CHECK_CLOSE) {
	if (self->lo_fd >= 0) {
	    PyErr_SetString(PyExc_IOError, "object is already opened.");
	    return 0;
	}
    }

    return 1;
}

/* --------------------------------------------------------------------- */
/* PG "LARGE" OBJECT IMPLEMENTATION */


/* constructor (internal use only) */
static pglargeobject *
pglarge_new(pgobject *pgcnx, Oid oid)
{
    pglargeobject *npglo;

    if ((npglo = PyObject_NEW(pglargeobject, &PglargeType)) == NULL)
	return NULL;

    Py_XINCREF(pgcnx);
    npglo->pgcnx = pgcnx;
    npglo->lo_fd = -1;
    npglo->lo_oid = oid;

    return npglo;
}

/* destructor */
static void
pglarge_dealloc(pglargeobject * self)
{
    if (self->lo_fd >= 0 && check_pg_obj(self->pgcnx))
	lo_close(self->pgcnx->cnx, self->lo_fd);
    self->lo_fd = -1;
    self->lo_oid = 0;
    Py_XDECREF(self->pgcnx);
    PyObject_Del(self);
}

/* opens large object */
static char pglarge_open__doc__[] =
"open(mode) -- open access to large object with specified mode "
"(INV_READ, INV_WRITE constants defined by module).";
static PyObject *
pglarge_open(pglargeobject *self, PyObject *args)
{
    int mode, fd;

    /* check validity */
    if (!check_lo_obj(self, CHECK_CLOSE))
	return NULL;

    /* gets arguments */
    if (!PyArg_ParseTuple(args, "i:open", &mode)) {
	    PyErr_SetString(PyExc_TypeError, "open(mode), with mode(integer).");
	    return NULL;
	}

    mode &= (INV_READ|INV_WRITE);
    /* opens large object */
    if ((fd = lo_open(self->pgcnx->cnx, self->lo_oid, mode)) < 0) {
	PyErr_SetString(PyExc_IOError, "can't open large object.");
	return NULL;
    }
    self->lo_fd = fd;

    /* no error : returns Py_None */
    Py_INCREF(Py_None);
    return Py_None;
}

/* close large object */
static char pglarge_close__doc__[] =
"close() -- close access to large object data.";
static PyObject *
pglarge_close(pglargeobject *self, PyObject *args)
{
    /* checks args */
    if (!PyArg_ParseTuple(args, ":close")) {
	PyErr_SetString(PyExc_TypeError,
			"method close() takes no parameters.");
	return NULL;
    }

    /* checks validity */
    if (!check_lo_obj(self, CHECK_OPEN))
	return NULL;

    /* closes large object */
    if (lo_close(self->pgcnx->cnx, self->lo_fd)) {
	PyErr_SetString(PyExc_IOError, "error while closing large object fd.");
	return NULL;
    }
    self->lo_fd = -1;

    /* no error : returns Py_None */
    Py_INCREF(Py_None);
    return Py_None;
}

/* reads from large object */
static char pglarge_read__doc__[] =
"read(integer) -- read from large object to sized string. "
"Object must be opened in read mode before calling this method.";
static PyObject *
pglarge_read(pglargeobject *self, PyObject *args)
{
    int		size;
    PyObject   *buffer;

    /* checks validity */
    if (!check_lo_obj(self, CHECK_OPEN))
	return NULL;

    /* gets arguments */
    if (!PyArg_ParseTuple(args, "i:read", &size)) {
	PyErr_SetString(PyExc_TypeError, "read(size), wih size (integer).");
	return NULL;
    }

    if (size <= 0) {
	PyErr_SetString(PyExc_ValueError, "size must be positive.");
	return NULL;
    }

    /* allocate buffer and runs read */
    buffer = PyString_FromStringAndSize((char *) NULL, size);

    if ((size = lo_read(self->pgcnx->cnx, self->lo_fd, BUF(buffer), size)) < 0) {
	PyErr_SetString(PyExc_IOError, "error while reading.");
	Py_XDECREF(buffer);
	return NULL;
    }

    /* resize buffer and returns it */
    _PyString_Resize(&buffer, size);
    return buffer;
}

/* write to large object */
static char pglarge_write__doc__[] =
"write(string) -- write sized string to large object. "
"Object must be opened in read mode before calling this method.";
static PyObject *
pglarge_write(pglargeobject *self, PyObject *args)
{
    char *buffer;
    int	size, bufsize;

    /* checks validity */
    if (!check_lo_obj(self, CHECK_OPEN))
	return NULL;

    /* gets arguments */
    if (!PyArg_ParseTuple(args, "s#:write", &buffer, &bufsize)) {
	PyErr_SetString(PyExc_TypeError,
			"write(buffer), with buffer (sized string).");
	return NULL;
    }

    /* sends query */
    if ((size = lo_write(self->pgcnx->cnx, self->lo_fd, buffer,
			 bufsize)) < bufsize) {
	PyErr_SetString(PyExc_IOError, "buffer truncated during write.");
	return NULL;
    }

    /* no error : returns Py_None */
    Py_INCREF(Py_None);
    return Py_None;
}

/* go to position in large object */
static char pglarge_seek__doc__[] =
"seek(off, whence) -- move to specified position. Object must be opened "
"before calling this method. whence can be SEEK_SET, SEEK_CUR or SEEK_END, "
"constants defined by module.";
static PyObject *
pglarge_lseek(pglargeobject * self, PyObject * args)
{
    /* offset and whence are initialized to keep compiler happy */
    int ret;
    int offset = 0;
    int whence = 0;

    /* checks validity */
    if (!check_lo_obj(self, CHECK_OPEN))
	return NULL;

    /* gets arguments */
    if (!PyArg_ParseTuple(args, "ii:lseek", &offset, &whence)) {
	PyErr_SetString(PyExc_TypeError,
			"lseek(offset, whence), with offset and whence (integers).");
	return NULL;
    }

    /* sends query */
    if ((ret = lo_lseek(self->pgcnx->cnx, self->lo_fd, offset, whence)) == -1) {
	PyErr_SetString(PyExc_IOError, "error while moving cursor.");
	return NULL;
    }

    /* returns position */
    return PyInt_FromLong(ret);
}

/* gets large object size */
static char pglarge_size__doc__[] =
"size() -- return large object size. "
"Object must be opened before calling this method.";
static PyObject *
pglarge_size(pglargeobject *self, PyObject *args)
{
    int start;
    int end;

    /* checks args */
    if (!PyArg_ParseTuple(args, ":size")) {
	PyErr_SetString(PyExc_TypeError,
			"method size() takes no parameters.");
	return NULL;
    }

    /* checks validity */
    if (!check_lo_obj(self, CHECK_OPEN))
	return NULL;

    /* gets current position */
    if ((start = lo_tell(self->pgcnx->cnx, self->lo_fd)) == -1) {
	PyErr_SetString(PyExc_IOError, "error while getting current position.");
	return NULL;
    }

    /* gets end position */
    if ((end = lo_lseek(self->pgcnx->cnx, self->lo_fd, 0, SEEK_END)) == -1) {
	PyErr_SetString(PyExc_IOError, "error while getting end position.");
	return NULL;
    }

    /* move back to start position */
    if ((start = lo_lseek(self->pgcnx->cnx, self->lo_fd, start, SEEK_SET)) == -1) {
	PyErr_SetString(PyExc_IOError,
			"error while moving back to first position.");
	return NULL;
    }

    /* returns size */
    return PyInt_FromLong(end);
}

/* gets large object cursor position */
static char pglarge_tell__doc__[] =
"tell() -- give current position in large object. "
"Object must be opened before calling this method.";
static PyObject *
pglarge_tell(pglargeobject * self, PyObject * args)
{
    int			start;

    /* checks args */
    if (!PyArg_ParseTuple(args, ":tell")) {
	PyErr_SetString(PyExc_TypeError,
			"method tell() takes no parameters.");
	return NULL;
    }

    /* checks validity */
    if (!check_lo_obj(self, CHECK_OPEN))
	return NULL;

    /* gets current position */
    if ((start = lo_tell(self->pgcnx->cnx, self->lo_fd)) == -1) {
	PyErr_SetString(PyExc_IOError, "error while getting position.");
	return NULL;
    }

    /* returns size */
    return PyInt_FromLong(start);
}

/* exports large object as unix file */
static char pglarge_export__doc__[] =
"export(string) -- export large object data to specified file. "
"Object must be closed when calling this method.";

static PyObject *
pglarge_export(pglargeobject *self, PyObject *args)
{
    char	   *name;

    /* checks validity */
    if (!check_lo_obj(self, CHECK_CLOSE))
	return NULL;

    /* gets arguments */
    if (!PyArg_ParseTuple(args, "s", &name)) {
	PyErr_SetString(PyExc_TypeError,
			"export(filename), with filename (string).");
	return NULL;
    }

    /* runs command */
    if (!lo_export(self->pgcnx->cnx, self->lo_oid, name)) {
	PyErr_SetString(PyExc_IOError, "error while exporting large object.");
	return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}

/* deletes a large object */
static char pglarge_unlink__doc__[] =
"unlink() -- destroy large object. "
"Object must be closed when calling this method.";

static PyObject *
pglarge_unlink(pglargeobject * self, PyObject * args)
{
    /* checks args */
    if (!PyArg_ParseTuple(args, ":unlink")) {
	PyErr_SetString(PyExc_TypeError,
			"method unlink() takes no parameters.");
	return NULL;
    }

    /* checks validity */
    if (!check_lo_obj(self, CHECK_CLOSE))
	return NULL;

    /* deletes the object, invalidate it on success */
    if (!lo_unlink(self->pgcnx->cnx, self->lo_oid)) {
	PyErr_SetString(PyExc_IOError, "error while unlinking large object");
	return NULL;
    }
    self->lo_oid = 0;

    Py_INCREF(Py_None);
    return Py_None;
}

/* large object methods */
static struct PyMethodDef pglarge_methods[] = {
	{"open", (PyCFunction) pglarge_open, METH_VARARGS, pglarge_open__doc__},
	{"close", (PyCFunction) pglarge_close, METH_VARARGS, pglarge_close__doc__},
	{"read", (PyCFunction) pglarge_read, METH_VARARGS, pglarge_read__doc__},
	{"write", (PyCFunction) pglarge_write, METH_VARARGS, pglarge_write__doc__},
	{"seek", (PyCFunction) pglarge_lseek, METH_VARARGS, pglarge_seek__doc__},
	{"size", (PyCFunction) pglarge_size, METH_VARARGS, pglarge_size__doc__},
	{"tell", (PyCFunction) pglarge_tell, METH_VARARGS, pglarge_tell__doc__},
	{"export",(PyCFunction) pglarge_export,METH_VARARGS,pglarge_export__doc__},
	{"unlink",(PyCFunction) pglarge_unlink,METH_VARARGS,pglarge_unlink__doc__},
	{NULL, NULL}
};

/* get attribute */
static PyObject *
pglarge_getattr(pglargeobject * self, char *name)
{
    /* list postgreSQL large object fields */

    /* associated pg connection object */
    if (!strcmp(name, "pgcnx")) {
	if (check_lo_obj(self, 0)) {
	    Py_INCREF(self->pgcnx);
	    return (PyObject *) (self->pgcnx);
	}

	Py_INCREF(Py_None);
	return Py_None;
    }

    /* large object oid */
    if (!strcmp(name, "oid")) {
	if (check_lo_obj(self, 0))
	    return PyInt_FromLong(self->lo_oid);

	Py_INCREF(Py_None);
	return Py_None;
    }

    /* error (status) message */
    if (!strcmp(name, "error"))
	return PyString_FromString(PQerrorMessage(self->pgcnx->cnx));

    /* attributes list */
    if (!strcmp(name, "__members__")) {
	PyObject   *list = PyList_New(3);

	if (list) {
	    PyList_SET_ITEM(list, 0, PyString_FromString("oid"));
	    PyList_SET_ITEM(list, 1, PyString_FromString("pgcnx"));
	    PyList_SET_ITEM(list, 2, PyString_FromString("error"));
	}

	return list;
    }

    /* module name */
    if (!strcmp(name, "__module__"))
	return PyString_FromString(MODULE_NAME);

    /* class name */
    if (!strcmp(name, "__class__"))
	return PyString_FromString("pglarge");

    /* seeks name in methods (fallback) */
    return Py_FindMethod(pglarge_methods, (PyObject *) self, name);
}

/* prints query object in human readable format */
static int
pglarge_print(pglargeobject *self, FILE *fp, int flags)
{
    if (!check_lo_obj(self, 0)) {
	fprintf(fp, "<Invalid/Unlinked large object>");
	return 0;
    }
    if (self->lo_fd >= 0) {
	fprintf(fp, "<Opened large object, oid=%ld>", (long) self->lo_oid);
	return 0;
    }
    fprintf(fp, "<Closed large object, oid=%ld>", (long) self->lo_oid);
    return 0;
}

/* object type definition */
staticforward PyTypeObject PglargeType = {
	PyObject_HEAD_INIT(NULL)
	0,				/* ob_size */
	"pglarge",			/* tp_name */
	sizeof(pglargeobject),		/* tp_basicsize */
	0,				/* tp_itemsize */

	/* methods */
	(destructor) pglarge_dealloc,	/* tp_dealloc */
	(printfunc) pglarge_print,	/* tp_print */
	(getattrfunc) pglarge_getattr,	/* tp_getattr */
	0,				/* tp_setattr */
	0,				/* tp_compare */
	0,				/* tp_repr */
	0,				/* tp_as_number */
	0,				/* tp_as_sequence */
	0,				/* tp_as_mapping */
	0,				/* tp_hash */
};

/* creates large object */
static char pg_locreate__doc__[] =
"locreate() -- creates a new large object in the database.";
static PyObject * pg_locreate(pgobject *self, PyObject * args)
{
    int mode = 0;
    Oid lo_oid = InvalidOid;

    /* checks validity */
    if (!check_pg_obj(self))
	return NULL;

    /* gets arguments */
    if (!PyArg_ParseTuple(args, "i:locreate", &mode)) {
	PyErr_SetString(PyExc_TypeError,
			"locreate(mode), with mode (integer).");
	return NULL;
    }

    /* creates large object */
    lo_oid = lo_creat(self->cnx, mode&(INV_READ|INV_WRITE));
    if (lo_oid == InvalidOid) {
	PyErr_SetString(OperationalError, "can't create large object.");
	return NULL;
    }
    return (PyObject *) pglarge_new(self, lo_oid);
}

/* init from already known oid */
static char pg_getlo__doc__[] =
"getlo(long) -- create a large object instance for the specified oid.";
static PyObject *pg_getlo(pgobject * self, PyObject *args)
{
    int lo_oid = InvalidOid;

    /* checks validity */
    if (!check_pg_obj(self))
	return NULL;

    /* gets arguments */
    if (!PyArg_ParseTuple(args, "i:getlo", &lo_oid)) {
	PyErr_SetString(PyExc_TypeError, "getlo(oid), with oid (integer).");
	return NULL;
    }

    if (!lo_oid) {
	PyErr_SetString(PyExc_ValueError, "the object oid can't be null.");
	return NULL;
    }

    /* creates object */
    return (PyObject *) pglarge_new(self, lo_oid);
}

/* import unix file */
static char pg_loimport__doc__[] =
"loimport(filepath) -- create a new large object from specified file.";
static PyObject *pg_loimport(pgobject *self, PyObject *args)
{
    char	*name;
    Oid		lo_oid;

    /* checks validity */
    if (!check_pg_obj(self))
	return NULL;

    /* gets arguments */
    if (!PyArg_ParseTuple(args, "s", &name)) {
	PyErr_SetString(PyExc_TypeError, "loimport(name), with name (string).");
	return NULL;
    }

    /* imports file and checks result */
    lo_oid = lo_import(self->cnx, name);
    if (lo_oid == 0) {
	PyErr_SetString(OperationalError, "can't create large object.");
	return NULL;
    }
    return (PyObject *) pglarge_new(self, lo_oid);
}

