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

/* implements support for COPY FROM STDIN bulk data load methods */
#define _xfree(x) free(x); x = NULL;

/* return a string representation of the column names suitable for
   COPY FROM STDIN */
static char *
_pg_inserttable_colnames(PyObject *cols)
{
    char *ret;
    int nrcols, i, retsize;

    /* short-circuit for the default case */
    if (cols == Py_None) {
        return strdup("");
    }
    /* columns has to be a list type */
    if (!PySequence_Check(cols) ) {
        PyErr_SetString(PyExc_TypeError, "columns has to be a tuple/list type");
        return NULL;
    }
    nrcols = PySequence_Size(cols);
    retsize = 3; /*()*/
    for (i=0; i<nrcols; i++) {
        PyObject *item = PySequence_GetItem(cols, i);
        if (!item || !PyString_Check(item)) {
            PyErr_SetString(ProgrammingError, "column names should be strings");
            Py_XDECREF(item);
            return NULL;
        }
        retsize += PyString_Size(item)+1;
        Py_DECREF(item);
    }
    if (!(ret = calloc(1, retsize))) {
        PyErr_SetString(PyExc_MemoryError, "out of memory constructing column names");
        return NULL;
    }
    *ret = '(';
    retsize = 1;
    /* loop again... */
    for (i=0; i<nrcols; i++) {
        PyObject *col = PySequence_GetItem(cols, i);
        strcat(ret, PyString_AsString(col));
        strcat(ret, ",");
        retsize += PyString_Size(col)+1;
        Py_DECREF(col);
    }
    /* fix the last comma */
    ret[retsize-1] = ')';
    return ret;
}

/* escape strings for insertion into bytea columns. can not use the
   libpq's PQescapeByteaConn because that one screws up escaping the
   single quote character. Postgresql sez it should be escaped as as
   double quote, but it turns out the server *stores* a double quote
   in the field, so we need to force octal representation instead.
   --gafton
*/
static char*
_pgbytea_escape(unsigned const char *src, int srclen)
{
    int i, newlen;
    char *iptr;
    char *dst;

    /* first, determine the size of the escape string we need to allocate */
    newlen = 0;
    for (i=0; i<srclen; i++) {
        unsigned char c = src[i];
        if (c<0x20 || c>0x7e || c=='\\' || c=='\'')
            newlen += 5; /* \\ooo mode */
        else
            newlen++; /* printable character */
    }
    if (!(dst = calloc(1, newlen+1)) ) {
        PyErr_SetString(PyExc_MemoryError,
                        "bytea_escape: can not allocate memory for bytea item");
        return NULL;
    }
    /* now copy the string... */
    iptr = dst;
    for (i=0; i<srclen; i++) {
        unsigned char c = src[i];
        if (c<0x20 || c>0x7e || c=='\\' || c=='\'') {
            sprintf(iptr, "\\\\%03o", c);
            iptr += 5; /* \\ooo mode */
        } else {
            *iptr++ = c;
        }
    }
    return dst;
}
/* escape regular strings for insertion via copy from stdin */
static char*
_pgstring_escape(unsigned const char *src, int srclen)
{
    int i, newlen;
    char *iptr;
    char *dst;

    newlen = 0;
    for (i = 0; i<srclen; i++) {
        unsigned char c = src[i];
        if (c=='\\' || c=='\n' || c=='\r' || c=='\t') {
            newlen += 2;
        } else if (c>=0x20 && c<=0x7e) {
            newlen++;
        } else {
            newlen += 4;
        }
    }
    if (!(dst = calloc(1, newlen+1)) ) {
        PyErr_SetString(PyExc_MemoryError,
                        "string_escape: can not allocate memory for bytea item");
        return NULL;
    }
    /* now copy the string... */
    iptr = dst;
    for (i=0; i<srclen ; i++) {
        unsigned char c = src[i];
        switch (c) {
            case '\\':
                sprintf(iptr, "\\\\");
                iptr += 2;
                break;
            case '\n':
                sprintf(iptr, "\\n");
                iptr += 2;
                break;
            case '\r':
                sprintf(iptr, "\\r");
                iptr += 2;
                break;
            case '\t':
                sprintf(iptr, "\\t");
                iptr += 2;
                break;
            default:
                if (c >= 0x20 && c <= 0x7e) {
                    *iptr++ = c;
                } else {
                    sprintf(iptr, "\\x%02x", c);
                    iptr += 4;
                }
                break;
        }
    }
    return dst;
}

/* encode a PyObject item for COPY FROM stdin */
static char *
_pg_inserttable_pyencode(pgobject *self, PyObject *item, Oid coltype)
{
    PyObject *tmp;
    char *valStr = NULL;

    if (item == Py_None) {
        return strdup("\\N");
    }
    if (PyString_Check(item) || PyUnicode_Check(item)) {
        char *tmpStr = NULL;
        Py_ssize_t tmpLen = 0;
        PyObject *itemStr = NULL;

        if (PyString_Check(item)) {
            PyString_AsStringAndSize(item, &tmpStr, &tmpLen);
        } else {
            itemStr = PyUnicode_AsUTF8String(item);
            PyString_AsStringAndSize(itemStr, &tmpStr, &tmpLen);
        }
        /* CRAPTASTIC: if we insert into a BYTEA column we have to use
           a different encoding from that used for (var)char column
           string types */
        if (coltype == BYTEAOID) {
            valStr = _pgbytea_escape(tmpStr, tmpLen);
        } else {
            valStr = _pgstring_escape(tmpStr, tmpLen);
        }
        Py_XDECREF(itemStr);
        return valStr;
    }
    if (PyInt_Check(item) || PyLong_Check(item) || PyFloat_Check(item)) {
        char *retStr;
        if (!(retStr = calloc(1, MAX_BUFFER_SIZE))) {
            PyErr_SetString(PyExc_MemoryError,
                            "can not allocate memory for int item");
            return NULL;
        }
        if (PyInt_Check(item))
            snprintf(retStr, MAX_BUFFER_SIZE-1, "%ld", PyInt_AsLong(item));
        else if (PyLong_Check(item))
            snprintf(retStr, MAX_BUFFER_SIZE-1, "%lld", PyLong_AsLongLong(item));
        else
            snprintf(retStr, MAX_BUFFER_SIZE-1, "%f", PyFloat_AsDouble(item));
        valStr = strdup(retStr);
        free(retStr);
        return valStr;
    }
    if (!(tmp = PyObject_Repr(item)) || !PyString_Check(tmp) ) {
        PyErr_SetString(ProgrammingError, "Unknown data type in row data");
        Py_XDECREF(tmp);
        return NULL;
    }
    /* we can handle string representations, so we should be safe to
       call pourselves recursively */
    valStr = _pg_inserttable_pyencode(self, tmp, coltype);
    Py_DECREF(tmp);
    return valStr;
}

static Oid *
_pg_inserttable_getcoldef(pgobject *self, const char *table, PyObject *cols)
{
    PGresult *result = NULL;
    char *colstr = NULL;
    char *buffer = NULL;
    int numcols = 0;
    int i;
    Oid *coltypes;

    /* retrieve the columns properties, the fast way */
    if (cols == Py_None) {
        colstr = strdup("*");
    } else {
        int col_len = 0;
        colstr = _pg_inserttable_colnames(cols);
        col_len = strlen(colstr);
        /* get rid of the surrounding () */
        memmove(colstr, colstr+1, col_len-1);
        colstr[col_len-2] = '\0';
    }
    if (!(buffer = calloc(1, strlen(table)+strlen(colstr)+25/*select  from  limit 0*/)) ) {
        PyErr_SetString(PyExc_MemoryError, "out of memory querying table props");
        _xfree(colstr);
        return NULL;
    }
    snprintf(buffer, MAX_BUFFER_SIZE-1, "select %s from %s limit 0", colstr, table);
    _xfree(colstr);

    Py_BEGIN_ALLOW_THREADS ;
    result = PQexec(self->cnx, buffer);
    Py_END_ALLOW_THREADS ;
    _xfree(buffer);

    switch(PQresultStatus(result)) {
        case PGRES_COMMAND_OK: /* we need tuples returned! */
            numcols = 0;
            break;
        case PGRES_TUPLES_OK:
            numcols = PQnfields(result);
            break;
        case PGRES_BAD_RESPONSE:
        case PGRES_FATAL_ERROR:
        case PGRES_NONFATAL_ERROR:
            PyErr_SetString(ProgrammingError, PQerrorMessage(self->cnx));
            numcols = -1;
            break;
        default:
            PyErr_SetString(InternalError, "internal error: unknown result status.");
            numcols = -2;
            break;
    }
    if (numcols <= 0) {
        if (!numcols)
            PyErr_SetString(InternalError, "could not obtain the number of columns for copy operation");
        PQclear(result);
        return NULL;
    }
    if (!(coltypes = calloc(numcols, sizeof(Oid))) ) {
        PyErr_SetString(PyExc_MemoryError, "could not allocate memory for column type return");
        PQclear(result);
        return NULL;
    }

    /* FIXME: if cols were not passed in, build it? */

    /* now figure out what columns do we have */
    for (i=0 ; i< numcols ; i++) {
        //printf("column=%s type=%d\n", PQfname(result, i), PQftype(result, i));
        coltypes[i] = PQftype(result, i);
    }
    PQclear(result);
    return coltypes;
}

/* insert table */
static char pg_inserttable__doc__[] =
"inserttable(str, tuple, list) -- insert rows from list into table str. "
"If tuple is None, the fields in the list must be in the same order as "
"in the table; otherwise tuple is a list of column names that map the "
"rows from list. ";
static PyObject *
pg_inserttable(pgobject *self, PyObject *args)
{
    PyObject        *rows, *row;
    char        *table;
    PyObject        *cols;
    int                ret;
    char        *colstr = NULL;
    PGresult        *result = NULL;
    int                nrcols = 0;
    char        *buffer = NULL;
    int                buflen = 0;
    Oid                *coltypes = NULL;
    PyObject        *iterRows = NULL;

    /* checks validity */
    if (!check_pg_obj(self))
        return NULL;
    /* gets arguments */
    if (!PyArg_ParseTuple(args, "sOO:inserttable", &table, &cols, &rows)) {
        PyErr_SetString(PyExc_TypeError,
                        "inserttable(table, colnames, content), with table(string), "
                        "columns(tuple) and content(list).");
        return NULL;
    }
    if (! strlen(table)) {
        PyErr_SetString(PyExc_TypeError, "table name needs to be a non-empty string");
        return NULL;
    }
    /* rows has got to be a list of some sort or an iterator */
    if (!(PySequence_Check(rows) || PyIter_Check(rows))) {
        PyErr_SetString(PyExc_TypeError, "rows have to be a list or iterator type");
        return NULL;
    }
    /* grab the columns definition */
    if (!(colstr = _pg_inserttable_colnames(cols))) {
        return NULL;
    }
    /* grab the table columns definition */
    if (!(coltypes = _pg_inserttable_getcoldef(self, table, cols)) ) {
        _xfree(colstr);
        return NULL;
    }
    /* allocate the query buffer */
    buflen = strlen(table) + strlen(colstr) + 20/*"copy from stdin "*/;
    if (!( buffer = malloc(buflen) )) {
        PyErr_SetString(PyExc_MemoryError, "can not allocate query buffer");
        goto bad_exit;
    }
    /* generate the query string */
    sprintf(buffer, "copy %s %s from stdin", table, colstr);
    _xfree(colstr);

    Py_BEGIN_ALLOW_THREADS ;
    result = PQexec(self->cnx, buffer);
    Py_END_ALLOW_THREADS ;
    _xfree(buffer);
    
    if (!result) {
        PyErr_SetString(PyExc_ValueError, PQerrorMessage(self->cnx));
        _xfree(coltypes);
        return NULL;
    }
    switch(PQresultStatus(result)) {
        case PGRES_COPY_IN:
            break; /* that's what we want */
        default:
            PyErr_SetString(InternalError, "internal error: unknown result status.");
            _xfree(coltypes);
            PQclear(result);
            return NULL;
    }
    PQclear(result);

    /* prepare the work buffer */
    if (!(buffer = calloc(1, MAX_BUFFER_SIZE))) {
        PyErr_SetString(ProgrammingError, "out of memory paring row data");
        _xfree(coltypes);
        return NULL;
    }
    buflen = MAX_BUFFER_SIZE;

    /* main loop is done with an iterator for convenience */
    if (!(iterRows = PyObject_GetIter(rows)) ) {
        PyErr_SetString(ProgrammingError, "can not iterate over the provided rows");
        goto bad_exit;
    }
    if (PySequence_Check(cols))
        nrcols = PySequence_Length(cols);
    while ((row = PyIter_Next(iterRows))) {
        int col;
        int crtlen = 0;

        /* validate the row */
        if (!PySequence_Check(row) || (nrcols && PyObject_Size(row) != nrcols) ) {
            PyErr_SetString(PyExc_TypeError, "row entries are of incorrect type or size");
            Py_DECREF(row);
            goto bad_exit;
        }
        nrcols = PySequence_Size(row);
        memset(buffer, 0, buflen);
        crtlen = 0;

        /* now build the insert line */
        for (col=0; col < nrcols ; col++) {
            PyObject *item;
            char *valStr = NULL;
            int   vallen = 0;
            if (!(item = PySequence_GetItem(row, col))) {
                PyErr_SetString(ProgrammingError, "can not read row element");
                Py_DECREF(row);
                goto bad_exit;
            }
            if (!(valStr = _pg_inserttable_pyencode(self, item, coltypes[col])) ) {
                //printf("Don't know how to handle data; %s\n", PyString_AsString(PyObject_Str(item)));
                //printf("Data col is: %d from %s\n", col, PyString_AsString(PyObject_Str(cols)));
                //printf("item type is: %s\n", PyString_AsString(PyObject_Str(PyObject_Type(item))));
                Py_DECREF(item);
                Py_DECREF(row);
                goto bad_exit;
            }
            Py_DECREF(item);
            vallen = strlen(valStr);
            if (vallen + crtlen + 1 > buflen - 1) {
                buflen = crtlen + vallen + 2; /* make room for the tab and zero termination */
                if (!(buffer = realloc(buffer, buflen))) {
                    PyErr_SetString(PyExc_MemoryError, "out of memory");
                    Py_DECREF(row);
                    free(valStr);
                    goto bad_exit;
                }
            }
            snprintf(buffer+crtlen, vallen+2, "%s\t", valStr);
            crtlen += vallen+1;
            free(valStr);
        }
        Py_DECREF(row);
        /* replace last tab with \n */
        buffer[crtlen-1] = '\n';
        /* send data */
        ret = PQputCopyData(self->cnx, buffer, crtlen);
        if (!ret || ret<0) { /* got an error sending data */
            PyErr_SetString(ProgrammingError, PQerrorMessage(self->cnx));
            goto bad_exit;
        }
        //printf("put %s: %s", table, buffer);
    }
    _xfree(buffer);
    /* ends COPY_IN operation */
    ret = PQputCopyEnd(self->cnx, NULL);
    if (!ret || ret<0) {
        PyErr_SetString(ProgrammingError, PQerrorMessage(self->cnx));
        goto bad_exit;
    }
    /* need to check for the final result of the copy command */
    while ( (result = PQgetResult(self->cnx) ) ) {
        switch (PQresultStatus(result)) {
            case PGRES_BAD_RESPONSE:
            case PGRES_FATAL_ERROR:
            case PGRES_NONFATAL_ERROR:
                PyErr_SetString(ProgrammingError, PQresultErrorMessage(result));
                PQclear(result);
                goto bad_exit;
            default:
                PQclear(result);
                continue;
        }
    }

    /* no error : returns nothing */
    Py_DECREF(iterRows);
    free(coltypes);
    Py_INCREF(Py_None);
    return Py_None;
 bad_exit:
    if (buffer)
        free(buffer);
    if (coltypes)
        free(coltypes);
    Py_XDECREF(iterRows);
    return NULL;
}
