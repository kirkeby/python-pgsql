/*
 * pgsql, written by Cristian Gafton (c) 2006-2007, rPath, Inc.
 *        gafton@rpath.com
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

/* Note: This should be linked against the same C runtime lib as Python */

#include "postgres.h"
#include "libpq-fe.h"
#include "libpq/libpq-fs.h"
#include "catalog/pg_type.h"

/* these will be defined in Python.h again: */
#undef _POSIX_C_SOURCE
#undef HAVE_STRERROR
#undef snprintf
#undef vsnprintf

#include <Python.h>

#include "pgsql.h"

static PyObject *Error, *Warning, *InterfaceError,
	*DatabaseError, *InternalError, *OperationalError, *ProgrammingError,
	*IntegrityError, *DataError, *NotSupportedError;

static const char *PyPgVersion = "0.9.7";
static char pg__doc__[] = "Simple Python interface to PostgreSQL DB";

/* default values */
#define MODULE_NAME			"_pgsql"
#define ARRAYSIZE			1

/* flags for object validity checks */
#define CHECK_OPEN		1 << 0
#define CHECK_CLOSE		1 << 1
#define CHECK_CNX		1 << 2
#define CHECK_RESULT		1 << 3
#define CHECK_DQL		1 << 4
#define CHECK_CONNID		1 << 5

/* query result types */
#define RESULT_EMPTY		1
#define RESULT_DML			2
#define RESULT_DDL			3
#define RESULT_DQL			4

#define MAX_BUFFER_SIZE 8192	/* maximum transaction size */

#ifndef NO_DIRECT
#define DIRECT_ACCESS	1		/* enables direct access functions */
#endif   /* NO_DIRECT */

#ifndef NO_LARGE
#define LARGE_OBJECTS	1		/* enables large objects support */
#endif   /* NO_LARGE */

/* --------------------------------------------------------------------- */

/* MODULE GLOBAL VARIABLES */

DL_EXPORT(void) init_pgsql(void);

/* --------------------------------------------------------------------- */
/* OBJECTS DECLARATION */

/* pg connection object */

typedef struct
{
    PyObject_HEAD
    PGconn	*cnx;		/* PostGres connection handle */
    PGresult	*last_result;	/* last result content */
    int		connid;		/* reconnect counter */
    PyObject	*notices;	/* server notices since last execution */
} pgobject;

staticforward PyTypeObject PgType;

#define is_pgobject(v) ((v)->ob_type == &PgType)

/* constructor */
static PyObject *
pgobject_New(void)
{
    pgobject	*pgobj;

    if ((pgobj = PyObject_NEW(pgobject, &PgType)) == NULL)
	return NULL;

    pgobj->last_result = NULL;
    pgobj->cnx = NULL;
    pgobj->connid = 0;
    pgobj->notices = NULL;
    return (PyObject *) pgobj;
}

/* destructor */
static void pg_dealloc(pgobject *self)
{
    if (self->cnx) {
	PQfinish(self->cnx);
	self->cnx = NULL;
    }
    Py_XDECREF(self->notices);
    self->notices = NULL;
    PyObject_Del(self);
}

/* pg source object */

typedef struct
{
    PyObject_HEAD
    int		connid;		/* the parent's connid count on creation */
    int		prepared;	/* is this a prepared cursor */
    pgobject	*pgcnx;		/* parent connection object */
    PGresult	*last_result;	/* last result content */
    int		result_type;	/* result type (DDL/DML/DQL) */
    long	arraysize;	/* array size for fetch method */
    int		current_row;	/* current selected row */
    int		max_row;	/* number of rows in the result */
    int		num_fields;	/* number of fields in each row */
    PyObject	*name;		/* name of the prepared query */
    PyObject	*query;		/* last query executed by a prepared stmt */
}	pgsourceobject;

staticforward PyTypeObject PgSourceType;

#define is_pgsourceobject(v) ((v)->ob_type == &PgSourceType)

/* constructor (internal use only) */
static pgsourceobject *
pgsource_new(pgobject * pgcnx)
{
    pgsourceobject *npgobj;

    if (!pgcnx->connid || !pgcnx->cnx) {
	PyErr_SetString(InternalError, "Invalid db object for cursor instantiation.");
	return NULL;
    }
    /* allocates new query object */
    if ((npgobj = PyObject_NEW(pgsourceobject, &PgSourceType)) == NULL)
	return NULL;
    /* initializes internal parameters */
    Py_XINCREF(pgcnx);
    npgobj->pgcnx = pgcnx;
    npgobj->last_result = NULL;
    npgobj->connid = pgcnx->connid;
    npgobj->arraysize = ARRAYSIZE;
    npgobj->current_row = npgobj->max_row = npgobj->num_fields = 0;
    npgobj->prepared = 0;
    npgobj->name = NULL;
    npgobj->query = NULL;
    return npgobj;
}

/* destructor */
static void
pgsource_dealloc(pgsourceobject * self)
{
    if (self->last_result)
	PQclear(self->last_result);
    Py_XDECREF(self->pgcnx);
    Py_XDECREF(self->name);
    Py_XDECREF(self->query);
    PyObject_Del(self);
}

/* for binding the query parameters */
typedef struct
{
    int		nParams;
    Oid		*paramTypes;
    char	**paramValues;
    int		*paramLengths;
    int		*paramFormats;
    int		*mustFree;
} pgparams;


/* --------------------------------------------------------------------- */
/* INTERNAL FUNCTIONS */


/* prints result (mostly useful for debugging) */
/* Note: This is a simplified version of the Postgres function PQprint().
 * PQprint() is not used because handing over a stream from Python to
 * Postgres can be problematic if they use different libs for streams.
 * Also, PQprint() is considered obsolete and may be removed sometime.
 */
static void
print_result(FILE *fout, const PGresult *res)
{
    int numrows, numcols;
    int *colsize;
    int row, col;


#define draw_line  \
    for (col=0; col<numcols; col++) { \
	int i; \
        for (i=0; i<=colsize[col]+1; i++) \
	    fputc('-', fout); \
	fputc('+', fout); \
    }; \
    fputc('\n', fout);

    /* do we have anything to print */
    numrows = PQntuples(res);
    numcols = PQnfields(res);
    if (numrows <= 0 || numcols <= 0)
	return;

    /* need to max size for each column */
    colsize = calloc(numcols, sizeof(int));
    for (col=0; col<numcols; col++) {
	char *colname;
	colname = PQfname(res, col);
	colsize[col] = strlen(colname);
	/* any of the data is longer in size? */
	for (row=0; row<numrows; row++) {
	    int vallen = PQgetlength(res, row, col);
	    if (vallen > colsize[col])
		colsize[col] = vallen;
	}
    }

    /* draw the dash line */
    draw_line ;

    /* print the header */
    for (col=0; col<numcols; col++)
	fprintf(fout, " %-*s |", colsize[col], PQfname(res, col));
    fputc('\n', fout);

    /* draw the dash line */
    draw_line ;

    /* print results */
    for (row=0; row<numrows; row++) {
	for (col=0; col<numcols; col++)
	    fprintf(fout, " %-*s |", colsize[col],
		    PQgetvalue(res, row, col));
	fputc('\n', fout);
    }

    /* draw the dash line */
    draw_line ;

    /* sum it up */
    fprintf(fout, "(%d row%s)\n", numrows, numrows == 1 ? "" : "s");
    free(colsize);
}

/* checks connection validity */
static int check_pg_obj(pgobject *self)
{
    if (!self || !is_pgobject(self)) {
	PyErr_SetString(IntegrityError, "Code bug: invalid db connection object");
	return 0;
    }
    if (!self->cnx) {
	PyErr_SetString(IntegrityError, "connection has been closed.");
	return 0;
    }
    return 1;
}

/* checks source object validity */
static int
check_source_obj(pgsourceobject *self, int level)
{
    if (!self || !is_pgsourceobject(self)) {
	PyErr_SetString(IntegrityError, "Code bug: invalid cursor object");
	return 0;
    }
    if (!self->connid) {
	PyErr_SetString(IntegrityError, "object has been closed");
	return 0;
    }
    if ((level & CHECK_RESULT) && self->last_result == NULL) {
	PyErr_SetString(DatabaseError, "no result.");
	return 0;
    }
    if ((level & CHECK_DQL) && self->result_type != RESULT_DQL) {
	PyErr_SetString(ProgrammingError, "last query did not return any rows.");
	return 0;
    }
    if ((level & CHECK_CNX) && !check_pg_obj(self->pgcnx))
	return 0;
    if ((level & CHECK_CONNID) && (self->connid != self->pgcnx->connid)) {
	PyErr_SetString(ProgrammingError,
			"Database connection was reset since cursor's creation.");
	return 0;
    }
    return 1;
}

/* checks for emtpy arguments for fucntions that don't take args */
static int check_no_args(PyObject *args, const char *fname)
{
    if (!args)
	return 1;
    /* check args */
    if (!PyArg_ParseTuple(args, "")) {
	PyObject *ret;
	if (fname) {
	    ret = PyString_FromString(fname);
	    PyString_ConcatAndDel(&ret, PyString_FromString("() "));
	} else {
	    ret = PyString_FromString("this ");
	}
	/* add the explanatory text */
	PyString_ConcatAndDel(&ret, PyString_FromString("method takes no parameters."));
	PyErr_SetObject(PyExc_TypeError, ret);
	Py_DECREF(ret);
	return 0;
    }
    return 1;
}

/* --------------------------------------------------------------------- */
/* PG SOURCE OBJECT IMPLEMENTATION */

/* clear the execution status of a source object */
static void _pg_source_clear(pgsourceobject *self)
{
    if (!self)
	return;
    if (self->last_result)
	PQclear(self->last_result);
    self->result_type = RESULT_EMPTY;
    self->last_result = NULL;
    self->max_row = 0;
    self->current_row = 0;
    self->num_fields = 0;
}

/* closes object */
static char pgsource_close__doc__[] =
"close() -- close query object without deleting it. "
"All instances of the query object can no longer be used after this call.";
static PyObject *
pgsource_close(pgsourceobject * self, PyObject * args)
{
    if (!check_source_obj(self, 0))
	return NULL;
    if (!check_no_args(args, "close"))
	return NULL;

    /* frees result if necessary and invalidates object */
    _pg_source_clear(self);
    self->connid = 0;

    /* if this was a prepared statement, we no longer need it */
    self->prepared = 0;
    Py_XDECREF(self->name);
    Py_XDECREF(self->query);
    self->query = NULL;
    self->name = NULL;

    /* return None */
    Py_INCREF(Py_None);
    return Py_None;
}

/* internal function to process the result of a PQexec* call */
static PyObject *_pgsource_postexec(pgsourceobject * self)
{
    const char *temp;

    /* checks result validity */
    if (!self->last_result) {
	PyErr_SetString(PyExc_ValueError, PQerrorMessage(self->pgcnx->cnx));
	return NULL;
    }

    /* checks result status */
    switch (PQresultStatus(self->last_result)) {
	long		num_rows;

	/* query succeeded */
	case PGRES_TUPLES_OK:	/* DQL: returns None (DB-SIG compliant) */
	    self->result_type = RESULT_DQL;
	    self->max_row = PQntuples(self->last_result);
	    self->num_fields = PQnfields(self->last_result);
	    self->current_row = 0;
	    Py_INCREF(Py_None);
	    return Py_None;
	case PGRES_COMMAND_OK:	/* other requests */
	case PGRES_COPY_OUT:
	case PGRES_COPY_IN:
	    self->result_type = RESULT_DDL;
	    temp = PQcmdTuples(self->last_result);
	    num_rows = -1;
	    if (temp[0] != 0) {
		self->result_type = RESULT_DML;
		num_rows = atol(temp);
	    }
	    self->max_row = num_rows;
	    return PyInt_FromLong(num_rows);

	    /* query failed */
	case PGRES_EMPTY_QUERY:
	    PyErr_SetString(PyExc_ValueError, "empty query.");
	    break;
	case PGRES_BAD_RESPONSE:
	case PGRES_FATAL_ERROR:
	case PGRES_NONFATAL_ERROR:
	    PyErr_SetString(ProgrammingError, PQerrorMessage(self->pgcnx->cnx));
	    break;
	default:
	    PyErr_SetString(InternalError, "internal error: "
			    "unknown result status.");
	    break;
    }

    /* frees result and returns error */
    _pg_source_clear(self);
    return NULL;
}

/* free a structure allocated by the pgsource_getparams internal call */
static void _pgsource_freeparams(pgparams *params)
{
    if (params == NULL)
	return;
    if (params->mustFree) {
	int i;
	for (i = 0; i < params->nParams; i++)
	    if (params->mustFree[i] &&
		params->paramValues && params->paramValues[i])
		free(params->paramValues[i]);
	free(params->mustFree);
    }
    if (params->paramTypes)
	free(params->paramTypes);
    if (params->paramValues)
	free(params->paramValues);
    if (params->paramLengths)
	free(params->paramLengths);
    if (params->paramFormats)
	free(params->paramFormats);
    free(params);
    return;
}

/* allocate a new pgparams structure */
static pgparams *_pgsource_newparams(int nParams)
{
    pgparams *ret;
    /* calloc zeroes out the memory allocated */
    ret = calloc(1, sizeof(pgparams));
    if (ret == NULL)
	return NULL;
    ret->nParams = nParams;
    if (ret->nParams == 0)
	return ret;
    ret->paramTypes = calloc(ret->nParams, sizeof(Oid));
    ret->paramValues = calloc(ret->nParams, sizeof(void *));
    ret->paramLengths = calloc(ret->nParams, sizeof(int));
    ret->paramFormats = calloc(ret->nParams, sizeof(int));
    ret->mustFree = calloc(ret->nParams, sizeof(int));
    if (ret->paramTypes == NULL || ret->paramValues == NULL ||
	ret->paramLengths == NULL || ret->paramFormats == NULL ||
	ret->mustFree == NULL) {
	_pgsource_freeparams(ret);
	return NULL;
    }
    return ret;
}
/* process a tuple/list containing the bind parameters for a query and
   return a structure that contains the necessary elements for a
   PQexecParams or PQexecPrepared call */
static pgparams *_pgsource_getparams(PyObject *params)
{
    pgparams *ret;
    int		i;

    if (params == Py_None)
	ret = _pgsource_newparams(0);
    else
	ret = _pgsource_newparams(PyObject_Length(params));
    if (ret == NULL)
	return NULL;

    for (i = 0; i < ret->nParams; i++) {
	PyObject *param;
	param = PySequence_GetItem(params, i);

	/* bool is s subclass of Int, so we need to check first for it */
	if (PyBool_Check(param)) {
	    if (param == Py_True)
		ret->paramValues[i] = "TRUE";
	    else
		ret->paramValues[i] = "FALSE";
	    ret->paramTypes[i] = BOOLOID;
	} else if (PyInt_Check(param) || PyLong_Check(param)) {
	    PyObject *str = PyObject_Str(param);
	    ret->paramValues[i] = strdup(PyString_AsString(str));
	    if (ret->paramValues[i] == NULL) {
		Py_DECREF(param);
		Py_DECREF(str);
		_pgsource_freeparams(ret);
		PyErr_SetString(ProgrammingError, "out of memory binding paramaters");
		return NULL;
	    }
	    ret->mustFree[i] = 1;
	    if (PyInt_Check(param))
		ret->paramTypes[i] = INT4OID;
	    else
		ret->paramTypes[i] = INT8OID;
	    Py_DECREF(str);
	} else if (PyString_Check(param)) {
	    Py_ssize_t len;
	    PyString_AsStringAndSize(param, &(ret->paramValues[i]), &len);
            ret->paramTypes[i] = BYTEAOID;
            ret->paramFormats[i] = 1;
	    ret->paramLengths[i] = len;
	} else if (PyUnicode_Check(param)) {
            /* Encode as UTF-8 */
            PyObject *str;
	    Py_ssize_t len;
            char *c;
            str = PyUnicode_AsUTF8String(param);
            PyString_AsStringAndSize(str, &c, &len);
            /* Clone UTF-8 string into buffer for libpq */
	    ret->paramValues[i] = (char *) malloc(len);
            if(ret->paramValues[i] == NULL) {
                Py_DECREF(str);
		_pgsource_freeparams(ret);
		PyErr_SetString(ProgrammingError, "out of memory binding paramaters");
		return NULL;
            }
            memcpy(ret->paramValues[i], c, len);
            /* Free temporary data and hand clone over to libpq */
            Py_DECREF(str);
            ret->mustFree[i] = 1;
	    ret->paramTypes[i] = VARCHAROID;
	    ret->paramFormats[i] = 1;
	    ret->paramLengths[i] = len;
	} else if (PyFloat_Check(param)) {
	    /* this is kind of lame - could not find any documentation
	       how to pass a double as a binary */
	    char *dblstr = malloc(128);
	    if (dblstr == NULL) {
		Py_DECREF(param);
		_pgsource_freeparams(ret);
		PyErr_SetString(ProgrammingError, "out of memory binding paramaters");
		return NULL;
	    }
	    snprintf(dblstr, 128, "%f", PyFloat_AsDouble(param));
	    ret->paramValues[i] = dblstr;
	    ret->mustFree[i] = 1;
	} else if (param == Py_None) {
	    ret->paramTypes[i] = 0;
	    ret->paramValues[i] = 0;
	} else { /* this is an object that hopefully we can str() */
	    PyObject *o = NULL;
	    PyObject *str = NULL;
	    Py_ssize_t len;
	    char *value;

	    /* is this an object that needs to be treated as a binary one? */
	    if (PyObject_HasAttrString(param, "__binary__"))
		ret->paramFormats[i] = 1;
	    /* try to quote the object */
	    if (PyObject_HasAttrString(param, "__pgquote__"))
		o = PyObject_GetAttrString(param, "__pgquote__");
	    else if (PyObject_HasAttrString(param, "__quote__"))
		o = PyObject_GetAttrString(param, "__quote__");

	    if (o != NULL && PyCallable_Check(o))
		str = PyObject_CallObject(o, NULL);
	    else /* hope for the best */
		str = PyObject_Str(param);

	    PyString_AsStringAndSize(str, &value, &len);
	    ret->paramValues[i] = calloc(1, len+1);
	    if (ret->paramValues[i] == NULL) {
		Py_XDECREF(o);
		Py_DECREF(str);
		Py_DECREF(param);
		_pgsource_freeparams(ret);
		PyErr_SetString(ProgrammingError, "out of memory binding paramaters");
		return NULL;
	    }
	    ret->mustFree[i] = 1;
	    memcpy(ret->paramValues[i], value, len);
	    ret->paramLengths[i] = len;
	    Py_XDECREF(o);
	    Py_DECREF(str);
	}
	Py_DECREF(param);
    }
    return ret;
}

/* internal function - convert singletons to tuples for binding */
static PyObject *_pg_item_astuple(PyObject *item)
{
    PyObject *tuple;

    if (PyList_Check(item) || PyTuple_Check(item)) {
	Py_INCREF(item);
	return item;
    }
    /* if not a list or tuple - attempt to convert if we can */
    if ( item != Py_None && !PyNumber_Check(item) && !PyString_Check(item) ) {
	PyObject *str;

	str = PyString_FromString("can not bind parameter type: ");
	PyString_ConcatAndDel(&str, PyObject_Str(item));
	PyErr_SetObject(ProgrammingError, str);

	Py_DECREF(str);
	return NULL;
    }

    /* create a tuple for the singleton */
    if ((tuple = PyTuple_New(1))==NULL) {
	PyErr_SetString(InternalError,
			"item_astuple: could not allocate element memory");
	return NULL;
    }
    Py_INCREF(item);
    PyTuple_SET_ITEM(tuple, 0, item);
    return tuple;
}

/* database query */
static char pgsource_execute__doc__[] =
"execute(sql[,params]) -- execute a SQL statement (string) optionally using parameters.\n "
"On success, this call returns the number of affected rows, "
"or None for DQL (SELECT, ...) statements.\n"
"The fetch (fetch(), fetchone() and fetchall()) methods can be used "
"to get result rows.";

static PyObject *
pgsource_execute(pgsourceobject *self, PyObject * args)
{
    char	*query;
    int		query_len;
    PyObject	*params = NULL;
    pgparams	*binds = NULL;
    int		ret;

    /* check cursor validity */
    if (!check_source_obj(self, self->prepared ? CHECK_CNX | CHECK_CONNID : CHECK_CNX))
	return NULL;

    /* if this is a prepared source, we only need params */
    if (self->prepared)
	ret = PyArg_ParseTuple(args, "O:execute", &params);
    else
	ret = PyArg_ParseTuple(args, "s#|O:execute", &query, &query_len, &params);
    if (!ret) {
	PyErr_SetString(PyExc_TypeError, "execute(sql[,params]), with sql(string) and params(tuple).");
	return NULL;
    }

    /* frees previous result */
    _pg_source_clear(self);

    /* do we need the parameter bind structure? */
    /* "binding" Py_None simulates no params */
    if (!params)
	params = Py_None;
    /* attempt to convert to a tuple, if we can */
    if (params != Py_None)
	if ((params = _pg_item_astuple(params)) == NULL ) {
	    PyErr_SetString(ProgrammingError, "execute with parameters requires params as a sequence");
	    return NULL;
	}

    if ((binds = _pgsource_getparams(params)) == NULL)
	return NULL;

    /* now run the query */
    Py_BEGIN_ALLOW_THREADS ;
    if (self->prepared) {
	if (self->name && PyString_Check(self->name))
	    query = PyString_AsString(self->name);
	else
	    query = "";
	self->last_result = PQexecPrepared(self->pgcnx->cnx,
					   query,
					   binds->nParams,
					   (const char **)binds->paramValues,
					   binds->paramLengths,
					   binds->paramFormats,
					   0);
    } else {
	self->last_result = PQexecParams(self->pgcnx->cnx,
					 query,
					 binds->nParams,
					 binds->paramTypes,
					 (const char **)binds->paramValues,
					 binds->paramLengths,
					 binds->paramFormats,
					 0);
    }
    Py_END_ALLOW_THREADS ;

    if (params != Py_None) {
	Py_DECREF(params);
    }
    _pgsource_freeparams(binds);
    return _pgsource_postexec(self);
}

/* faster-path command execution for queries that do not return rows */
static char pgsource_query__doc__[] =
"execute(sql) -- execute a SQL statement.\n "
"On success, this call returns the number of affected rows, "
"or None for DQL (SELECT, ...) statements.\n";
static PyObject *
pgsource_query(pgsourceobject *self, PyObject * args)
{
    char	*query;
    int		query_len;

    /* check cursor validity */
    if (!check_source_obj(self, CHECK_CNX))
	return NULL;

    if (!PyArg_ParseTuple(args, "s#:query", &query, &query_len)) {
	PyErr_SetString(PyExc_TypeError, "query(sql), with sql(string).");
	return NULL;
    }

    /* frees previous result */
    _pg_source_clear(self);

    /* now run the query */
    Py_BEGIN_ALLOW_THREADS ;
    self->last_result = PQexec(self->pgcnx->cnx, query);
    Py_END_ALLOW_THREADS ;

    return _pgsource_postexec(self);
}

/* helper function for checking the results of the executemany */
static int _pg_result_check(PGconn *conn, PGresult *result)
{
    int failed;

    if (!result) {
	PyErr_SetString(PyExc_ValueError, PQerrorMessage(conn));
	return 0;
    }
    /* check result status */
    switch (PQresultStatus(result)) {
	/* these are the only return values we accept as success */
	case PGRES_COMMAND_OK:
	case PGRES_TUPLES_OK:
	    /* all cool */
	    failed = 0;
	    break;
	case PGRES_EMPTY_QUERY:
	    PyErr_SetString(PyExc_ValueError, "empty query.");
	    failed = 1;
	    break;
	    /* query failed */
	case PGRES_BAD_RESPONSE:
	case PGRES_FATAL_ERROR:
	case PGRES_NONFATAL_ERROR:
	    PyErr_SetString(ProgrammingError, PQerrorMessage(conn));
	    failed = 1;
	    break;
	default:
	    failed = 1;
	    PyErr_SetString(InternalError, "internal error: unknown result status.");
	    break;
    }
    if (failed) {
	/* frees result and returns error */
	PQclear(result);
	return 0;
    }
    return 1;
}

/* bulk database ops */
static char pgsource_executemany__doc__[] =
"execute(sql, params) -- execute a SQL statement (string) using parameters.\n ";
static PyObject *
pgsource_executemany(pgsourceobject * self, PyObject * args)
{
    char	*query = NULL;
    int		query_len = 0;
    PyObject	*paramsList = NULL;
    PGresult	*prep = NULL;
    int		ret;
    PyObject	*iterator = NULL;
    PyObject	*item = NULL;

    /* check cursor validity */
    if (!check_source_obj(self, self->prepared ? CHECK_CNX | CHECK_CONNID : CHECK_CNX))
	return NULL;

    /* if this is a prepared source, we can't executemany */
    if (self->prepared)
	ret = PyArg_ParseTuple(args, "O:execute", &paramsList);
    else
	ret = PyArg_ParseTuple(args, "s#O:execute", &query, &query_len, &paramsList);
    /* params must be a sequence or an iterator */
    if ( !ret || paramsList == Py_None ||
	 !(PySequence_Check(paramsList) || PyIter_Check(paramsList)) ) {
	PyErr_SetString(PyExc_TypeError, "execute(sql,params), with sql(string) and params(list of tuples).");
	return NULL;
    }

    /* frees previous result */
    _pg_source_clear(self);

    if (!self->prepared) {
	/* we need to prepare a statement to execute for the paramsList */
	Py_BEGIN_ALLOW_THREADS ;
	prep = PQprepare(self->pgcnx->cnx, "", query, 0, NULL);
	Py_END_ALLOW_THREADS ;

	if (!_pg_result_check(self->pgcnx->cnx, prep))
	    return NULL;
    }

    /* we have now our prepared statement, loop over the paramList */
    iterator = PyObject_GetIter(paramsList);
    if (!iterator) {
	PyErr_SetString(ProgrammingError, "can not iterate over the provided params list");
	if (prep)
	    PQclear(prep);
	return NULL;
    }

    /* now loop over the params and execute the prepared query */
    while ((item = PyIter_Next(iterator))) {
	pgparams	*binds = NULL;
	PyObject	*tuple;

	if ((tuple = _pg_item_astuple(item)) == NULL ) {
	    PyObject *str;

	    str = PyString_FromString("can not bind parameter type: ");
	    PyString_ConcatAndDel(&str, PyObject_Str(item));
	    PyErr_SetObject(ProgrammingError, str);
	    Py_DECREF(str);

	    Py_DECREF(item);
	    Py_DECREF(iterator);
	    if (prep)
		PQclear(prep);
	    return NULL;
	}

	binds = _pgsource_getparams(tuple);
	/* now run the query */
	if (self->prepared) {
	    if (self->name && PyString_Check(self->name))
		query = PyString_AsString(self->name);
	    else
		query = "";
	} else {
	    query = "";
	}
	/* free previous result */
	_pg_source_clear(self);

	Py_BEGIN_ALLOW_THREADS ;
	self->last_result = PQexecPrepared(self->pgcnx->cnx,
					   query,
					   binds->nParams,
					   (const char **)binds->paramValues,
					   binds->paramLengths,
					   binds->paramFormats,
					   0);
	Py_END_ALLOW_THREADS ;

	/* clean up */
	_pgsource_freeparams(binds);
	Py_DECREF(item);
	Py_DECREF(tuple);

	if (!_pg_result_check(self->pgcnx->cnx, self->last_result)) {
	    self->last_result = NULL;
	    Py_DECREF(iterator);
	    if (prep)
		PQclear(prep);
	    return NULL;
	}
    }
    Py_DECREF(iterator);
    if (prep)
	PQclear(prep);
    return _pgsource_postexec(self);
}

/* FETCHING DATA from a PGresult */
static PyObject *
_pg_fetch_cell(PGresult *result, int row, int col)
{
    PyObject	*ret;
    char	*cell;
    Oid		oidtype;
    unsigned int cellsize = 0;

    if (PQgetisnull(result, row, col)) {
	Py_INCREF(Py_None);
	return Py_None;
    }

    cell = PQgetvalue(result, row, col);
    cellsize = PQgetlength(result, row, col);
    switch ((oidtype = PQftype(result, col))) {
	case BOOLOID:
	    if (*cell == 't' || *cell == 'T') {
		ret = Py_True;
		Py_INCREF(Py_True);
	    } else {
		ret = Py_False;
		Py_INCREF(Py_False);
	    }
	    break;
	case INT2OID:
	case INT4OID:
	    ret = PyInt_FromString(cell, NULL, 10);
	    break;
	case INT8OID:
	case OIDOID:
	case XIDOID:
	    ret = PyLong_FromString(cell, NULL, 10);
	    break;
	case NUMERICOID:
	case FLOAT8OID:
	case FLOAT4OID:
	    {
		PyObject *tmp;
		tmp = PyString_FromString(cell);
		ret = PyFloat_FromString(tmp, NULL);
		Py_DECREF(tmp);
	    }
	    break;
	case CASHOID: /* nasty $-x,yyy.zzz format */
	    {
		int cashsign = 1;
		char *cashbuf = malloc(PQgetlength(result, row, col) + 1);
		char *s = cell;
		int k = 0;

		/* get rid of the '$' and commas */
		for (k=0 ; *s ; s++) {
		    if (*s == '$' || *s ==',' || *s == ' ' || *s == ')') continue;
		    if (*s == '-' || *s == '(') {
			cashsign = -1;
			continue;
		    }
		    cashbuf[k++] = *s;
		}
		cashbuf[k] = 0;
		ret = PyFloat_FromDouble(strtod(cashbuf, NULL) * cashsign);
		free(cashbuf);
	    }
	    break;
        /* Decoding these is handled by the Python code */
	case DATEOID:
	case ABSTIMEOID:
	case RELTIMEOID:
        case TIMESTAMPOID:
	case TIMESTAMPTZOID:
	case TINTERVALOID:
	case INTERVALOID:
	case TIMEOID:
	case TIMETZOID:
        /* .. and these are actual character string types */
	case BPCHAROID:
	case VARCHAROID:
	case NAMEOID:
	case CHAROID:
	case TEXTOID:
	    /* It is assumed that null character terminates these strings */
	    ret = PyUnicode_DecodeUTF8(cell, cellsize, "strict");
	    break;
	case BYTEAOID:
	    if (PQfformat(result, col) == 0) {
		/* bytea values returned in text mode need to be decoded */
		char *newStr;
		size_t newLen;
		newStr = PQunescapeBytea(cell, &newLen);
		ret = PyString_FromStringAndSize(newStr, (Py_ssize_t)newLen);
		PQfreemem(newStr);
		break;
	    }
	    /* fall-through on else is intentional */
	default:
	    {
		void *tmpstr = NULL;
		Py_ssize_t tmplen = 0;
		int retcode = 0;
		fprintf(stderr, "WARNING: UNKNOWN DATATYPE %ld processed as string. "
			"Check pg_type.h to decode this OID\n", (long)oidtype);
		if (!(ret = PyBuffer_New(cellsize)) || ret == PyExc_ValueError) {
		    PyErr_SetString(PyExc_MemoryError, "could not allocate buffer object for row data");
		    return NULL;
		}
		retcode = PyObject_AsWriteBuffer(ret, &tmpstr, &tmplen);
		if (retcode < 0 || (tmplen != cellsize)) {
		    PyErr_SetString(InternalError, "could not convert unknown data field to a buffer object");
		    Py_DECREF(ret);
		    return NULL;
		}
		memcpy(tmpstr, cell, cellsize);
		break;
	    }
    }
    return ret;
}

/* internal function for getting one result row as a python tuple */
static PyObject *_pg_result_rowtuple(PGresult *result, int row)
{
    PyObject	*rowtuple;
    int		num_fields, col;

    num_fields = PQnfields(result);
    /* allocate list for result */
    if ((rowtuple = PyTuple_New(num_fields)) == NULL)
	return NULL;
    for (col = 0; col < num_fields; col++) {
	PyObject *cell;
	cell = _pg_fetch_cell(result, row, col);
	if (cell == NULL) {
	    Py_DECREF(rowtuple);
	    return NULL;
	}
	PyTuple_SET_ITEM(rowtuple, col, cell);
    }
    return rowtuple;
}
/* internal function for getting one result row as a python dict */
static PyObject *_pg_result_rowdict(PGresult *result, int row)
{
    PyObject	*rowdict;
    int		num_fields, col;

    num_fields = PQnfields(result);
    /* allocate list for result */
    if ((rowdict = PyDict_New()) == NULL)
	return NULL;
    for (col = 0; col < num_fields; col++) {
	PyObject *cell;
	cell = _pg_fetch_cell(result, row, col);
	if (cell == NULL) {
	    Py_DECREF(rowdict);
	    return NULL;
	}
	PyDict_SetItemString(rowdict, PQfname(result, col), cell);
	Py_DECREF(cell);
    }
    return rowdict;
}

/* fetches next row from last result as a tuple*/
static char pgsource_fetchone__doc__[] =
"fetchone() -- return the row from the last result as a tuple. ";
static PyObject *
pgsource_fetchone(pgsourceobject * self, PyObject * args)
{
    PyObject	*rowtuple;

    if (!check_source_obj(self, CHECK_RESULT | CHECK_DQL))
	return NULL;
    if (!check_no_args(args, "fetchone"))
	return NULL;

    if (self->current_row >= self->max_row) {
	Py_INCREF(Py_None);
	return Py_None;
    }
    if ((rowtuple = _pg_result_rowtuple(self->last_result, self->current_row)) == NULL)
	return NULL;
    self->current_row++;
    return rowtuple;
}

/* fetches next row from last result as a dict*/
static char pgsource_fetchonedict__doc__[] =
"fetchonedict() -- return the row from the last result as a dict.";
static PyObject *
pgsource_fetchonedict(pgsourceobject * self, PyObject * args)
{
    PyObject	*rowdict;

    if (!check_source_obj(self, CHECK_RESULT | CHECK_DQL))
	return NULL;
    if (!check_no_args(args, "fetchonedict"))
	return NULL;

    if (self->current_row >= self->max_row) {
	Py_INCREF(Py_None);
	return Py_None;
    }

    if ((rowdict = _pg_result_rowdict(self->last_result, self->current_row)) == NULL)
	return NULL;
    self->current_row++;
    return rowdict;
}

/* retrieves all remaining results as a list of tuples */
static char pgsource_fetchall__doc__[] =
"fetchall() -- Gets the result of a query.  The result is returned "
"as a list of rows, each one a list of fields in the order returned "
"by the server.";
static PyObject *
pgsource_fetchall(pgsourceobject * self, PyObject * args)
{
    int	row;
    PyObject *reslist;

    if (!check_source_obj(self, CHECK_RESULT | CHECK_DQL))
	return NULL;
    if (!check_no_args(args, "fetchall"))
	return NULL;

    /* need to return an empty list ? */
    if (self->current_row > self->max_row) {
	/* already returned the empty list, another fetchall call is invalid... */
	Py_INCREF(Py_None);
	return Py_None;
    } else if (self->current_row == self->max_row) {
	self->current_row++;
	return PyList_New(0);
    }

    /* stores result in presized list for efficiency */
    reslist = PyList_New(self->max_row - self->current_row);

    /* return the remaining rows that have not been "extracted" yet */
    for (row = self->current_row; row < self->max_row; row++) {
	PyObject *rowtuple;
	if ((rowtuple = _pg_result_rowtuple(self->last_result, row)) == NULL) {
	    Py_DECREF(reslist);
	    return NULL;
	}
	PyList_SET_ITEM(reslist, row - self->current_row, rowtuple);
    }
    /* mark all rows returned */
    self->current_row = self->max_row + 1;
    /* returns list */
    return reslist;
}

/* retrieves all remaining results as a list of dictionaries*/
static char pgsource_fetchalldict__doc__[] =
"fetchalldict() -- Gets the result of a query.  The result is returned "
"as a list of dictionaries with the field names used as the keys.";
static PyObject *
pgsource_fetchalldict(pgsourceobject * self, PyObject * args)
{
    int	row;
    PyObject *reslist;

    if (!check_source_obj(self, CHECK_RESULT | CHECK_DQL))
	return NULL;
    if (!check_no_args(args, "fetchall"))
	return NULL;

    /* need to return an empty list ? */
    if (self->current_row > self->max_row) {
	/* already returned the empty list, another fetchall call is invalid... */
	Py_INCREF(Py_None);
	return Py_None;
    } else if (self->current_row == self->max_row) {
	self->current_row++;
	return PyList_New(0);
    }

    /* stores result in presized list for efficiency */
    reslist = PyList_New(self->max_row - self->current_row);

    /* return the remaining rows that have not been "extracted" yet */
    for (row = self->current_row; row < self->max_row; row++) {
	PyObject *rowdict;
	if ((rowdict = _pg_result_rowdict(self->last_result, row)) == NULL) {
	    Py_DECREF(reslist);
	    return NULL;
	}
	PyList_SET_ITEM(reslist, row - self->current_row, rowdict);
    }
    /* mark all rows returned */
    self->current_row = self->max_row + 1;
    /* returns list */
    return reslist;
}

/* finds field number from string/integer (internal use only) */
static int
pgsource_fieldindex(pgsourceobject * self, PyObject *param, const char *usage)
{
    int			num;

    /* checks validity */
    if (!check_source_obj(self, CHECK_RESULT | CHECK_DQL))
	return -1;

    /* gets field number */
    if (PyString_Check(param))
	num = PQfnumber(self->last_result, PyString_AsString(param));
    else if (PyInt_Check(param))
	num = PyInt_AsLong(param);
    else {
	PyErr_SetString(PyExc_TypeError, usage);
	return -1;
    }

    /* checks field validity */
    if (num < 0 || num >= self->num_fields) {
	PyErr_SetString(PyExc_ValueError, "Unknown field.");
	return -1;
    }
    return num;
}

/* convert PG type OIDs to Python types. Return a PyInt for stuff we don't know */
static PyObject *_pgsource_typecode(int typecode)
{
    PyObject *tc;
    switch (typecode) {
	case INT2OID:
	case INT4OID:
	    tc = PyString_FromString("integer");
	    break;
	case INT8OID:
	    tc = PyString_FromString("long");
	    break;
	case OIDOID:
	case XIDOID:
	    tc = PyString_FromString("oid");
	    break;
	case NUMERICOID:
	case FLOAT8OID:
	case FLOAT4OID:
	    tc = PyString_FromString("double");
	    break;
	case BYTEAOID:
	    tc = PyString_FromString("binary");
	    break;
	case NAMEOID:
	case CHAROID:
	case BPCHAROID:
	case TEXTOID:
	case VARCHAROID:
	    tc = PyString_FromString("string");
	    break;
	case CASHOID:
	    tc = PyString_FromString("money");
	    break;
	case DATEOID:
	    tc = PyString_FromString("date");
	    break;
	case ABSTIMEOID:
	case RELTIMEOID:
	case TIMESTAMPOID:
	case TIMESTAMPTZOID:
	    tc = PyString_FromString("datetime");
	    break;
        /*
	case TINTERVALOID:
	case INTERVALOID:
	case TIMEOID:
	case TIMETZOID:
        */
	case BOOLOID:
	    tc = PyString_FromString("bool");
	    break;
	default:
	    tc = PyInt_FromLong(typecode);
	    break;
    }
    return tc;
}

/* describe one column, per DB API 2.0 */
static char pgsource_fieldinfo__doc__[] =
"fieldinfo(string|integer) -- return specified field information "
"(name, type_code, display_size, internal_size, precision, scale, null_ok).";
static PyObject *_pg_result_fieldinfo(PGresult *result, int field)
{
    PyObject   *info;

    if ((info = PyTuple_New(7)) == NULL) {
	return NULL;
    }

    /* name */
    PyTuple_SET_ITEM(info, 0,
		     PyString_FromString(PQfname(result, field)));
    /* type code */
    PyTuple_SET_ITEM(info, 1,
		     _pgsource_typecode(PQftype(result, field)));
    /* display size */
    Py_INCREF(Py_None);
    PyTuple_SET_ITEM(info, 2, Py_None);
    /* internal size */
    PyTuple_SET_ITEM(info, 3,
		     PyInt_FromLong(PQfsize(result, field)));
    /* precision, scale, null_ok - we dunno */
    Py_INCREF(Py_None);
    PyTuple_SET_ITEM(info, 4, Py_None);
    Py_INCREF(Py_None);
    PyTuple_SET_ITEM(info, 5, Py_None);
    Py_INCREF(Py_None);
    PyTuple_SET_ITEM(info, 6, Py_None);

    return info;
}

static PyObject *
pgsource_fieldinfo(pgsourceobject *self, PyObject * args)
{
    static const char short_usage[] =
	"fieldinfo(field), with field (string|integer).";
    int		num;
    PyObject	*param;

    /* gets args */
    if (!PyArg_ParseTuple(args, "O", &param)) {
	PyErr_SetString(PyExc_TypeError, short_usage);
	return NULL;
    }

    /* checks args and validity */
    if ((num = pgsource_fieldindex(self, param, short_usage)) == -1)
	return NULL;
    /* returns result */
    return _pg_result_fieldinfo(self->last_result, num);
};


/* describe all columns, per DB API 2.0
   Each of these sequences contains information describing one result
   column: (name, type_code, display_size, internal_size, precision,
            scale, null_ok) */
static PyObject *_pg_source_description(pgsourceobject *self)
{
    int        i;
    PyObject   *result;

    /* checks validity */
    if (!check_source_obj(self, CHECK_RESULT))
	return NULL;

    /* if the last query did not return rows, give up */
    if (self->result_type != RESULT_DQL) {
	Py_INCREF(Py_None);
	return Py_None;
    }

    /* builds result */
    if ((result = PyTuple_New(self->num_fields)) == NULL)
	return NULL;

    for (i = 0; i < self->num_fields; i++) {
	PyObject   *info;

	if ((info = _pg_result_fieldinfo(self->last_result, i)) == NULL) {
	    Py_DECREF(result);
	    return NULL;
	}
	/* all done for this element */
	PyTuple_SET_ITEM(result, i, info);
    }

    /* returns result */
    return result;
};

/* list fields names from query result */
static PyObject * _pg_result_fields(PGresult *result)
{
    int		i, n;
    char	*name;
    PyObject   *fieldstuple;

    if (result == NULL) {
	Py_INCREF(Py_None);
	return Py_None;
    }

    /* builds tuple */
    n = PQnfields(result);
    fieldstuple = PyTuple_New(n);

    for (i = 0; i < n; i++) {
	PyObject *str;
	name = PQfname(result, i);
	str = PyString_FromString(name);
	PyTuple_SET_ITEM(fieldstuple, i, str);
    }
    return fieldstuple;
}

/* query object methods */
static PyMethodDef pgsource_methods[] = {
	{"close", (PyCFunction) pgsource_close, METH_VARARGS,
			pgsource_close__doc__},
	{"execute", (PyCFunction) pgsource_execute, METH_VARARGS,
			pgsource_execute__doc__},
	{"query", (PyCFunction) pgsource_query, METH_VARARGS,
			pgsource_query__doc__},
	{"executemany", (PyCFunction) pgsource_executemany, METH_VARARGS,
			pgsource_executemany__doc__},
	{"fetchone", (PyCFunction) pgsource_fetchone, METH_VARARGS,
			pgsource_fetchone__doc__},
	{"fetchonedict", (PyCFunction) pgsource_fetchonedict, METH_VARARGS,
			pgsource_fetchonedict__doc__},
	{"fieldinfo", (PyCFunction) pgsource_fieldinfo, METH_VARARGS,
			pgsource_fieldinfo__doc__},
	{"fetchall", (PyCFunction) pgsource_fetchall, METH_VARARGS,
			pgsource_fetchall__doc__},
	{"fetchalldict", (PyCFunction) pgsource_fetchalldict, METH_VARARGS,
			pgsource_fetchalldict__doc__},
	{NULL, NULL}
};

/* gets query object attributes */
static PyObject *
pgsource_getattr(pgsourceobject * self, char *name)
{
    /* pg connection object */
    if (!strcmp(name, "connection")) {
	if (check_source_obj(self, 0)) {
	    Py_INCREF(self->pgcnx);
	    return (PyObject *) (self->pgcnx);
	}
	Py_INCREF(Py_None);
	return Py_None;
    }

    /* arraysize */
    if (!strcmp(name, "arraysize"))
	return PyInt_FromLong(self->arraysize);
    /* resulttype */
    if (!strcmp(name, "resulttype"))
	return PyInt_FromLong(self->result_type);
    /* ntuples */
    if (!strcmp(name, "rowcount")) {
        return PyInt_FromLong(self->max_row);
    }
    /* nfields */
    if (!strcmp(name, "nfields")) {
	if (self->result_type != RESULT_DQL)
	    return PyInt_FromLong(-1);
	else
	    return PyInt_FromLong(self->num_fields);
    }
    /* rownumber */
    if (!strcmp(name, "rownumber")) {
	if (self->result_type != RESULT_DQL)
	    return PyInt_FromLong(-1);
	else
	    return PyInt_FromLong(self->current_row);
    }
    /* fields */
    if (!strcmp(name, "fields")) {
	if (self->result_type != RESULT_DQL)
	    return PyTuple_New(0);
	else
	    return _pg_result_fields(self->last_result);
    }
    /* description */
    if (!strcmp(name, "description"))
	return _pg_source_description(self);
    /* available server notices */
    if (!strcmp(name, "notices")) {
	if (!self->pgcnx->notices)
	    return PyList_New(0);
	Py_INCREF(self->pgcnx->notices);
	return self->pgcnx->notices;
    }
    /* oidstatus */
    if (!strcmp(name, "oidstatus")) {
	Oid		oid;
	/* retrieves oid status */
	if ((oid = PQoidValue(self->last_result)) == InvalidOid) {
	    Py_INCREF(Py_None);
	    return Py_None;
	}
	return PyInt_FromLong(oid);
    }
    /* description */
    if (!strcmp(name, "valid")) {
	if (check_source_obj(self, CHECK_CNX | CHECK_CONNID)) {
	    Py_INCREF(Py_True);
	    return Py_True;
	}
	Py_INCREF(Py_False);
	return Py_False;
    }
    /* attributes list */
    if (!strcmp(name, "__members__")) {
	static char *members[] = {
	    "connection", "arraysize", "resulttype", "rowcount", "nfields",
	    "rownumber", "fields", "notices", "description", "oidstatus",
	    "valid", NULL};
	int i = 0;
	PyObject *list;

	for (i=0; members[i] ; i++);
	list = PyList_New(i);
	for (i=0 ; members[i] ; i++)
	    PyList_SET_ITEM(list, i, PyString_FromString(members[i]));
	return list;
    }

    /* module name */
    if (!strcmp(name, "__module__"))
	return PyString_FromString(MODULE_NAME);

    /* class name */
    if (!strcmp(name, "__class__"))
	return PyString_FromString("pgsource");

    /* seeks name in methods (fallback) */
    return Py_FindMethod(pgsource_methods, (PyObject *) self, name);
}

/* sets query object attributes */
static int
pgsource_setattr(pgsourceobject * self, char *name, PyObject * v)
{
    /* arraysize */
    if (!strcmp(name, "arraysize")) {
	if (PyInt_Check(v))
	    self->arraysize = PyInt_AsLong(v);
	else if (PyLong_Check(v))
	    self->arraysize = PyLong_AsLong(v);
	else {
	    PyErr_SetString(PyExc_TypeError, "arraysize must be integer.");
	    return -1;
	}
	return 0;
    }

    /* unknown attribute */
    PyErr_SetString(PyExc_TypeError, "not a writable attribute.");
    return -1;
}

/* prints query object in human readable format */
static int
pgsource_print(pgsourceobject *self, FILE *fp, int flags)
{
    if (self->prepared) {
	char *name, *query;
	name = query = "";
	if (self->name && PyString_Check(self->name))
	    name = PyString_AsString(self->name);
	if (self->query && PyString_Check(self->query))
	    query = PyString_AsString(self->query);
	fprintf(fp, "<pgsql prepared cursor '%s' for query>:\n%s\n", name, query);
    } else {
	fprintf(fp, "<pgsql cursor>\n");
    }

    fprintf(fp, "Last Execution Result:\n");
    switch (self->result_type) {
	case RESULT_DQL:
	    print_result(fp, self->last_result);
	    break;
	case RESULT_DDL:
	case RESULT_DML:
	    fprintf(fp, "%s\n", PQcmdStatus(self->last_result));
	    break;
	case RESULT_EMPTY:
	default:
	    fprintf(fp, "Empty PostgreSQL source object.\n");
	    break;
    }
    return 0;
}

/* query type definition */
staticforward PyTypeObject PgSourceType = {
	PyObject_HEAD_INIT(NULL)

	0,				/* ob_size */
	"pgsourceobject",		/* tp_name */
	sizeof(pgsourceobject),		/* tp_basicsize */
	0,				/* tp_itemsize */
	/* methods */
	(destructor) pgsource_dealloc,	/* tp_dealloc */
	(printfunc) pgsource_print,	/* tp_print */
	(getattrfunc) pgsource_getattr, /* tp_getattr */
	(setattrfunc) pgsource_setattr, /* tp_setattr */
	0,				/* tp_compare */
	0,				/* tp_repr */
	0,				/* tp_as_number */
	0,				/* tp_as_sequence */
	0,				/* tp_as_mapping */
	0,				/* tp_hash */
};


/* --------------------------------------------------------------------- */
/* generic notice processor callback*/
static void _pg_notice_callback(pgobject *self, char *message)
{
    PyObject *msg;
    if (!check_pg_obj(self)) {
	/* Hmm.. Since this is a callback, there is no useful way for
	   us to raise an exception here - nor should we do it,
	   ebcause the notice is not really an error message for the
	   query being executed. The default processor will do */
	fprintf(stderr, "WARNING: invalid connection while processing notice:\n%s",
		message);
	return;
    }
    /* we do not want notices recorded */
    if (!self->notices || !PyList_Check(self->notices))
	return;
    msg = PyString_FromString(message);
    PyList_Append(self->notices, msg);
    Py_DECREF(msg);
}

/* connects to a database */
static char connect__doc__[] =
"connect(dbname, host, port, opt, tty) -- connect to a PostgreSQL database "
"using specified parameters (optionals, keywords aware).";

static PyObject *
pgconnect(pgobject * self, PyObject * args, PyObject * dict)
{
    static char *kwlist[] = {
	"dbname", "user", "passwd", "host", "port", "opt", "tty", NULL};
    pgobject   *npgobj;

    char *pgdbname = NULL;
    char *pghost   = NULL;
    char *pguser   = NULL;
    char *pgpasswd = NULL;
    char *pgopt    = NULL;
    char *pgtty    = NULL;
    int	  pgport   = -1;

    if (!PyArg_ParseTupleAndKeywords(args, dict, "|zzzzizz:connect", kwlist,
				     &pgdbname, &pguser, &pgpasswd, &pghost,
				     &pgport, &pgopt, &pgtty))
	return NULL;

    if ((npgobj = (pgobject *) pgobject_New()) == NULL)
	return NULL;

    if (pgport != -1) {
	char	port_buffer[20];
	memset(port_buffer, 0, sizeof(port_buffer));
	snprintf(port_buffer, sizeof(port_buffer), "%d", pgport);
	npgobj->cnx = PQsetdbLogin(pghost, port_buffer, pgopt, pgtty,
				   pgdbname, pguser, pgpasswd);
    } else
	npgobj->cnx = PQsetdbLogin(pghost, NULL, pgopt, pgtty,
				   pgdbname, pguser, pgpasswd);

    if (PQstatus(npgobj->cnx) == CONNECTION_BAD) {
	PyErr_SetString(ProgrammingError, PQerrorMessage(npgobj->cnx));
	PQfinish(npgobj->cnx);
	npgobj->cnx = NULL; /*noop*/
	Py_XDECREF(npgobj);
	return NULL;
    }
    npgobj->connid++;
    /* set the notice processor */
    PQsetNoticeProcessor(npgobj->cnx, (PQnoticeProcessor)_pg_notice_callback, npgobj);
    return (PyObject *) npgobj;
}

/* pgobject methods */

/* close without deleting */
static char pg_close__doc__[] =
"close() -- close connection. All instances of the connection object and "
"derived objects (queries and large objects) can no longer be used after "
"this call.";

static PyObject *
pg_close(pgobject *self, PyObject * args)
{
    if (!check_pg_obj(self))
	return NULL;
    if (!check_no_args(args, "close"))
	return NULL;

    PQfinish(self->cnx);
    self->cnx = NULL;
    self->connid = 0;
    /* give up the server notices */
    Py_XDECREF(self->notices);
    self->notices = NULL;
    Py_INCREF(Py_None);
    return Py_None;
}

/* resets connection */
static char pg_reset__doc__[] =
"reset() -- reset connection with current parameters. All derived queries "
"and large objects derived from this connection will not be usable after "
"this call.";

static PyObject *
pg_reset(pgobject * self, PyObject * args)
{
    if (!check_pg_obj(self))
	return NULL;
    if (!check_no_args(args, "reset"))
	return NULL;

    /* resets the connection */
    PQreset(self->cnx);
    self->connid++;
    Py_INCREF(Py_None);
    return Py_None;
}

/* cancels current command */
static char pg_cancel__doc__[] =
"cancel() -- abandon processing of the current command.";

static PyObject *
pg_cancel(pgobject * self, PyObject * args)
{
    if (!check_pg_obj(self))
	return NULL;
    if (!check_no_args(args, "cancel"))
	return NULL;

    /* request that the server abandon processing of the current command */
    return PyInt_FromLong((long) PQrequestCancel(self->cnx));
}

/* source creation */
static char pg_source__doc__[] =
"source() -- creates a new source object for this connection";
static PyObject *
pg_source(pgobject *self, PyObject *args)
{
    if (!check_pg_obj(self))
	return NULL;
    if (!check_no_args(args, "source"))
	return NULL;

    /* allocate new pg query object */
    return (PyObject *) pgsource_new(self);
}

/* prepared statement creation */
static char pg_prepare__doc__[] =
"prepare(sql) -- prepares a statement for execution and returns a cursor "
"bound to the prepared statement. sql is a string that accepts bind arguments.";

static PyObject *
pg_prepare(pgobject *self, PyObject *args)
{
    char	*query;
    int		querylen;
    pgsourceobject	*src;
    char	*stmt = NULL;
    int		stmt_len = 0;
    char	*tmp = NULL;

    /* checks validity */
    if (!check_pg_obj(self))
	return NULL;

    /* checks args */
    if (!PyArg_ParseTuple(args, "s#|s#:prepare", &query, &querylen, &tmp, &stmt_len)) {
	PyErr_SetString(PyExc_TypeError, "prepare(query[,name]), where query,name are strings");
	return NULL;
    }

    /* we need to make sure that stmt is all lowercased */
    if (tmp && *tmp) {
	if ((stmt = malloc(stmt_len+1)) == NULL) {
	    PyErr_SetString(InternalError, "can not allocate memory");
	    return NULL;
	}
	memset(stmt, '\0', stmt_len+1);
	strncpy(stmt, tmp, stmt_len);
	for (tmp = stmt; *tmp; tmp++)
	    if (isalpha(*tmp))
		*tmp = tolower(*tmp);
    } else {
	stmt = "";
	stmt_len = 0;
    }

    /* allocate new pg query object */
    src = pgsource_new(self);

    /* prepare the statement */
    Py_BEGIN_ALLOW_THREADS ;
    src->last_result = PQprepare(self->cnx, stmt, query, 0, NULL);
    Py_END_ALLOW_THREADS ;

    /* checks result validity */
    if (!src->last_result) {
	PyErr_SetString(PyExc_ValueError, PQerrorMessage(self->cnx));
	if (stmt_len)
	    free(stmt);
	Py_DECREF(src);
	return NULL;
    }

    /* checks result status */
    switch (PQresultStatus(src->last_result)) {
	/* these are the only return values we accept as success */
	case PGRES_COMMAND_OK:
	case PGRES_TUPLES_OK: /* this should not appear */
	    src->result_type = RESULT_EMPTY; /* this is just a prepare */
	    src->prepared = 1;
	    src->query = PyString_FromStringAndSize(query, querylen);
	    if (stmt) {
		src->name = PyString_FromString(stmt);
	    } else {
		Py_INCREF(Py_None);
		src->name = Py_None;
	    }
	    if (stmt_len)
		free(stmt);
	    return (PyObject *)src;
	/* everything else is a failure... */
	case PGRES_EMPTY_QUERY:
	    PyErr_SetString(PyExc_ValueError, "empty query.");
	    break;
	/* query failed */
	case PGRES_BAD_RESPONSE:
	case PGRES_FATAL_ERROR:
	case PGRES_NONFATAL_ERROR:
	    PyErr_SetString(ProgrammingError, PQerrorMessage(self->cnx));
	    break;
	default:
	    PyErr_SetString(InternalError, "internal error: "
			    "unknown result status.");
	    break;
    }
    /* frees result and returns error */
    PQclear(src->last_result);
    src->last_result = NULL;
    Py_DECREF(src);
    if (stmt_len)
	free(stmt);
    return NULL;
}

/* database query */
static char pg_execute__doc__[] =
"query(sql[,params]) -- creates a new query object for this connection,"
" using sql (string) request and optionally positional params.";
static PyObject *
pg_execute(pgobject *self, PyObject *args)
{
    pgsourceobject	*src;
    PyObject	*ret;

    if (!check_pg_obj(self))
	return NULL;

    /* frees previous result */
    if (self->last_result) {
	PQclear(self->last_result);
	self->last_result = NULL;
    }

    /* this is very similar to pgsource_execute, so we need to get
       a pgsourceobject and get going */
    src = pgsource_new(self);
    ret = pgsource_execute(src, args);
    if (ret == NULL) {
	Py_DECREF(src);
	return NULL;
    }

    /* if we got rows back, construct and return a queryobject */
    if (ret == Py_None && PQresultStatus(src->last_result) == PGRES_TUPLES_OK) {
	Py_DECREF(ret);
	return (PyObject *)src;
    }
    Py_DECREF(src);
    return ret;
}

/* escape string */
static char pg_escape_string__doc__[] =
"escape_string(str) -- escape a string for use within SQL.";
static PyObject *
pg_escape_string(pgobject *self, PyObject *args) {
    char	*from;		/* our string argument */
    int		from_length;	/* length of string */
    char	*to = NULL;	/* the result */
    size_t	to_length = 0;	/* length of result */
    PyObject	*ret;		/* string object to return */
    int		err_code;

    /* need a valid connection to perform this escaping */
    if (!check_pg_obj(self))
	return NULL;

    if (!PyArg_ParseTuple(args, "s#:escape_string", &from, &from_length)) {
	PyErr_SetString(ProgrammingError, "escape_string(s), where s is a string");
	return NULL;
    }
    /* grab a new buffer for the escaped string */
    to = malloc(2*from_length + 1);
    if (to == NULL) {
	PyErr_SetString(InternalError, "cann not allocate required memory");
	return NULL;
    }
    /* escape the string */
    to_length = PQescapeStringConn(self->cnx, to, from, from_length, &err_code);
    if (err_code) {
	/* could not escape the string */
	PyErr_SetString(ProgrammingError, PQerrorMessage(self->cnx));
	free(to);
	return NULL;
    }
    ret = Py_BuildValue("s#", to, to_length);
    free(to);
    return ret;
}

/* escape bytea */
static char pg_escape_bytea__doc__[] =
"escape_bytea(data) -- escape binary data for use within SQL as type bytea.";
static PyObject *
pg_escape_bytea(pgobject *self, PyObject *args) {
    unsigned char	*from;		/* our string argument */
    int			from_length;	/* length of string */
    unsigned char	*to = NULL;	/* the result */
    size_t		to_length = 0;	/* length of result */
    PyObject		*ret;		/* string object to return */

    /* need a valid connection to perform this escaping */
    if (!check_pg_obj(self))
	return NULL;

    if (!PyArg_ParseTuple(args, "s#:escape_bytea", &from, &from_length)) {
	PyErr_SetString(ProgrammingError, "escape_bytea(s), where s is a string");
	return NULL;
    }
    to = PQescapeByteaConn(self->cnx, from, from_length, &to_length);
    if (!to) {
	/* could not escape the string */
	PyErr_SetString(ProgrammingError, PQerrorMessage(self->cnx));
	return NULL;
    }
    ret = Py_BuildValue("s#", to, to_length);
    PQfreemem(to);
    return ret;
}

/* unescape bytea */
static char pg_unescape_bytea__doc__[] =
"unescape_bytea(str) -- unescape bytea data that has been retrieved as text.";
static PyObject *
pg_unescape_bytea(pgobject *self, PyObject *args)
{
    unsigned char	*from;		/* our string argument */
    int			from_length;	/* source length */
    unsigned char	*to = NULL;	/* the result */
    size_t		to_length = 0;	/* length of result string */
    PyObject		*ret;	/* string object to return */

    /* need a valid connection to perform this escaping */
    if (!check_pg_obj(self))
	return NULL;

    if (!PyArg_ParseTuple(args, "s#:unescape_bytea", &from, &from_length)) {
	PyErr_SetString(ProgrammingError, "unescape_bytea(s), where s is a string");
	return NULL;
    }
    to = PQunescapeBytea(from, &to_length);
    if (!to) {
	/* could not escape the string */
	PyErr_SetString(ProgrammingError, PQerrorMessage(self->cnx));
	return NULL;
    }
    ret = Py_BuildValue("s#", to, to_length);
    PQfreemem(to);
    return ret;
}

static char pg_setnotices__doc__[] =
"setnotices(bool) - enables/disable receiving and storing of the server notices.\n"
"If enabled, the .notices attribute will be populated with server notice strings "
"after executes.";
static PyObject *
pg_setnotices(pgobject *self, PyObject *args)
{
    PyObject *val;

    if (!check_pg_obj(self))
	return NULL;

    if (!PyArg_ParseTuple(args, "O:setnotices", &val)) {
	PyErr_SetString(ProgrammingError, "setnotices(bool) requires a bool argument");
	return NULL;
    }

    /* reset the message queue */
    Py_XDECREF(self->notices);
    self->notices = NULL;

    /* do we need to start a new one */
    if (PyObject_Not(val)) {
	/* return False if we disable */
	Py_INCREF(Py_False);
	return Py_False;
    }
    if ((self->notices = PyList_New(0)) == NULL) {
	PyErr_SetString(InternalError, "Out of memory enabling server notice capture");
	return NULL;
    }
    /* ok, now notices will be captured */
    Py_INCREF(Py_True);
    return Py_True;
}

#ifdef LARGE_OBJECTS
#include "pglarge.c"
#endif /* LARGE_OBJECTS */

#ifdef DIRECT_ACCESS
#include "pgcopy.c"
#endif

/* connection object methods */
static struct PyMethodDef pgobj_methods[] = {
	{"source", (PyCFunction) pg_source, METH_VARARGS, pg_source__doc__},
	{"prepare", (PyCFunction) pg_prepare, METH_VARARGS, pg_prepare__doc__},
	{"execute", (PyCFunction) pg_execute, METH_VARARGS, pg_execute__doc__},
	{"reset", (PyCFunction) pg_reset, METH_VARARGS, pg_reset__doc__},
	{"cancel", (PyCFunction) pg_cancel, METH_VARARGS, pg_cancel__doc__},
	{"close", (PyCFunction) pg_close, METH_VARARGS, pg_close__doc__},
	{"escape_string", (PyCFunction) pg_escape_string, METH_VARARGS,
			pg_escape_string__doc__},
	{"escape_bytea", (PyCFunction) pg_escape_bytea, METH_VARARGS,
			pg_escape_bytea__doc__},
	{"unescape_bytea", (PyCFunction) pg_unescape_bytea, METH_VARARGS,
			pg_unescape_bytea__doc__},

#ifdef LARGE_OBJECTS
	{"locreate", (PyCFunction) pg_locreate, 1, pg_locreate__doc__},
	{"getlo",    (PyCFunction) pg_getlo, 1, pg_getlo__doc__},
	{"loimport", (PyCFunction) pg_loimport, 1, pg_loimport__doc__},
#endif   /* LARGE_OBJECTS */
#ifdef DIRECT_ACCESS
	{"bulkload",  (PyCFunction) pg_inserttable, METH_VARARGS, pg_inserttable__doc__},
#endif
	{"setnotices", (PyCFunction) pg_setnotices, METH_VARARGS, pg_setnotices__doc__},

	{NULL, NULL}				/* sentinel */
};

/* get transaction state */
static PyObject *
_pg_transaction(pgobject *self)
{
    if (!check_pg_obj(self))
	return NULL;
    return PyInt_FromLong(PQtransactionStatus(self->cnx));
}

/* get attribute */
static PyObject *
pg_getattr(pgobject * self, char *name)
{
    /*
     * Although we could check individually, there are only a few
     * attributes that don't require a live connection and unless someone
     * has an urgent need, this will have to do
     */

    /* first exception - close which returns a different error */
    if (strcmp(name, "close") && !check_pg_obj(self))
	return NULL;

    /* list postgreSQL connection fields */

    /* postmaster host */
    if (!strcmp(name, "host")) {
	char *r = PQhost(self->cnx);
	return r ? PyString_FromString(r) : PyString_FromString("localhost");
    }

    /* postmaster port */
    if (!strcmp(name, "port"))
	return PyInt_FromLong(atol(PQport(self->cnx)));

    /* selected database */
    if (!strcmp(name, "dbname"))
	return PyString_FromString(PQdb(self->cnx));

    /* selected options */
    if (!strcmp(name, "opt"))
	return PyString_FromString(PQoptions(self->cnx));

    /* selected postgres tty */
    if (!strcmp(name, "tty"))
	return PyString_FromString(PQtty(self->cnx));

    /* error (status) message */
    if (!strcmp(name, "error"))
	return PyString_FromString(PQerrorMessage(self->cnx));

    /* connection status : 1 - OK, 0 - BAD */
    if (!strcmp(name, "status"))
	return PyInt_FromLong(PQstatus(self->cnx) == CONNECTION_OK ? 1 : 0);

    /* available server notices */
    if (!strcmp(name, "notices")) {
	if (!self->notices)
	    return PyList_New(0);
	Py_INCREF(self->notices);
	return self->notices;
    }
    /* transaction status */
    if (!strcmp(name, "transaction"))
	return _pg_transaction(self);

    /* attributes list */
    if (!strcmp(name, "__members__")) {
	static char *members[] = {
	    "host", "port", "dbname", "opt", "tty", "error", "status",
	    "notices", "transaction", NULL};
	int i = 0;
	PyObject *list;

	for (i=0; members[i] ; i++);
	list = PyList_New(i);
	for (i=0 ; members[i] ; i++)
	    PyList_SET_ITEM(list, i, PyString_FromString(members[i]));
	return list;
    }

    return Py_FindMethod(pgobj_methods, (PyObject *) self, name);
}

/* set attributes */
static int
pg_setattr(pgobject * self, char *name, PyObject *v)
{
    /* unknown attribute */
    PyErr_SetString(PyExc_TypeError, "not a writable attribute.");
    return -1;
}

/* object type definition */
staticforward PyTypeObject PgType = {
	PyObject_HEAD_INIT(NULL)
	0,				/* ob_size */
	"pgobject",			/* tp_name */
	sizeof(pgobject),		/* tp_basicsize */
	0,				/* tp_itemsize */
	/* methods */
	(destructor) pg_dealloc,	/* tp_dealloc */
	0,				/* tp_print */
	(getattrfunc) pg_getattr,	/* tp_getattr */
	(setattrfunc) pg_setattr,	/* tp_setattr */
	0,				/* tp_compare */
	0,				/* tp_repr */
	0,				/* tp_as_number */
	0,				/* tp_as_sequence */
	0,				/* tp_as_mapping */
	0,				/* tp_hash */
};


/* --------------------------------------------------------------------- */

/* MODULE FUNCTIONS */

/* List of functions defined in the module */

static struct PyMethodDef pg_methods[] = {
	{"connect", (PyCFunction) pgconnect, METH_VARARGS|METH_KEYWORDS,
			connect__doc__},
	{NULL, NULL}				/* sentinel */
};

/* Initialization function for the module */
DL_EXPORT(void)
init_pgsql(void)
{
	PyObject   *mod, *dict, *v;

	/* Initialize here because some WIN platforms get confused otherwise */
	PgType.ob_type = PgSourceType.ob_type = &PyType_Type;
#ifdef LARGE_OBJECTS
	PglargeType.ob_type = &PyType_Type;
#endif /* LARGE_OBJECTS */

	/* Create the module and add the functions */
	mod = Py_InitModule4("_pgsql", pg_methods, pg__doc__, NULL, PYTHON_API_VERSION);
	dict = PyModule_GetDict(mod);

	/* Exceptions as defined by DB-API 2.0 */
	Error = PyErr_NewException("pgsql.Error", PyExc_StandardError, NULL);
	PyDict_SetItemString(dict, "Error", Error);

	Warning = PyErr_NewException("pgsql.Warning", PyExc_StandardError, NULL);
	PyDict_SetItemString(dict, "Warning", Warning);

	InterfaceError = PyErr_NewException("pgsql.InterfaceError", Error, NULL);
	PyDict_SetItemString(dict, "InterfaceError", InterfaceError);

	DatabaseError = PyErr_NewException("pgsql.DatabaseError", Error, NULL);
	PyDict_SetItemString(dict, "DatabaseError", DatabaseError);

	InternalError = PyErr_NewException("pgsql.InternalError", DatabaseError, NULL);
	PyDict_SetItemString(dict, "InternalError", InternalError);

	OperationalError =
		PyErr_NewException("pgsql.OperationalError", DatabaseError, NULL);
	PyDict_SetItemString(dict, "OperationalError", OperationalError);

	ProgrammingError =
		PyErr_NewException("pgsql.ProgrammingError", DatabaseError, NULL);
	PyDict_SetItemString(dict, "ProgrammingError", ProgrammingError);

	IntegrityError =
		PyErr_NewException("pgsql.IntegrityError", DatabaseError, NULL);
	PyDict_SetItemString(dict, "IntegrityError", IntegrityError);

	DataError = PyErr_NewException("pgsql.DataError", DatabaseError, NULL);
	PyDict_SetItemString(dict, "DataError", DataError);

	NotSupportedError =
		PyErr_NewException("pgsql.NotSupportedError", DatabaseError, NULL);
	PyDict_SetItemString(dict, "NotSupportedError", NotSupportedError);

	/* Make the version available */
	v = PyString_FromString(PyPgVersion);
	PyDict_SetItemString(dict, "version", v);
	PyDict_SetItemString(dict, "__version__", v);
	Py_DECREF(v);

	/* results type for queries */
	PyDict_SetItemString(dict, "RESULT_EMPTY", PyInt_FromLong(RESULT_EMPTY));
	PyDict_SetItemString(dict, "RESULT_DML", PyInt_FromLong(RESULT_DML));
	PyDict_SetItemString(dict, "RESULT_DDL", PyInt_FromLong(RESULT_DDL));
	PyDict_SetItemString(dict, "RESULT_DQL", PyInt_FromLong(RESULT_DQL));

	PyDict_SetItemString(dict,"TRANS_IDLE",PyInt_FromLong(PQTRANS_IDLE));
	PyDict_SetItemString(dict,"TRANS_ACTIVE",PyInt_FromLong(PQTRANS_ACTIVE));
	PyDict_SetItemString(dict,"TRANS_INTRANS",PyInt_FromLong(PQTRANS_INTRANS));
	PyDict_SetItemString(dict,"TRANS_INERROR",PyInt_FromLong(PQTRANS_INERROR));
	PyDict_SetItemString(dict,"TRANS_UNKNOWN",PyInt_FromLong(PQTRANS_UNKNOWN));

#ifdef LARGE_OBJECTS
	/* create mode for large objects */
	PyDict_SetItemString(dict, "INV_READ", PyInt_FromLong(INV_READ));
	PyDict_SetItemString(dict, "INV_WRITE", PyInt_FromLong(INV_WRITE));
	/* position flags for lo_lseek */
	PyDict_SetItemString(dict, "SEEK_SET", PyInt_FromLong(SEEK_SET));
	PyDict_SetItemString(dict, "SEEK_CUR", PyInt_FromLong(SEEK_CUR));
	PyDict_SetItemString(dict, "SEEK_END", PyInt_FromLong(SEEK_END));
#endif   /* LARGE_OBJECTS */

	/* Check for errors */
	if (PyErr_Occurred())
		Py_FatalError("can't initialize module _pgsql");
}
