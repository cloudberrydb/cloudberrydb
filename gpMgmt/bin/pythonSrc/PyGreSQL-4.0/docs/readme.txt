==========================================
PyGreSQL - Python interface for PostgreSQL
==========================================

--------------------
PyGreSQL version 4.0
--------------------

.. meta::
   :description: PyGreSQL - Python interface for PostgreSQL
   :keywords: PyGreSQL, PostGreSQL, Python

.. contents:: Contents


Copyright notice
================

Written by D'Arcy J.M. Cain (darcy@druid.net)

Based heavily on code written by Pascal Andre (andre@chimay.via.ecp.fr)

Copyright (c) 1995, Pascal Andre

Further modifications copyright (c) 1997-2008 by D'Arcy J.M. Cain
(darcy@druid.net)

Permission to use, copy, modify, and distribute this software and its
documentation for any purpose, without fee, and without a written agreement
is hereby granted, provided that the above copyright notice and this
paragraph and the following two paragraphs appear in all copies or in any
new file that contains a substantial portion of this file.

IN NO EVENT SHALL THE AUTHOR BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT,
SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS,
ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF THE
AUTHOR HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

THE AUTHOR SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED
TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND THE
AUTHOR HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
ENHANCEMENTS, OR MODIFICATIONS.


Introduction
============

**PostgreSQL** is a highly scalable, SQL compliant, open source
object-relational database management system. With more than 15 years
of development history, it is quickly becoming the de facto database
for enterprise level open source solutions.
Best of all, PostgreSQL's source code is available under the most liberal
open source license: the BSD license.

**Python** Python is an interpreted, interactive, object-oriented
programming language. It is often compared to Tcl, Perl, Scheme or Java.
Python combines remarkable power with very clear syntax. It has modules,
classes, exceptions, very high level dynamic data types, and dynamic typing.
There are interfaces to many system calls and libraries, as well as to
various windowing systems (X11, Motif, Tk, Mac, MFC). New built-in modules
are easily written in C or C++. Python is also usable as an extension
language for applications that need a programmable interface.
The Python implementation is copyrighted but freely usable and distributable,
even for commercial use.

**PyGreSQL** is a Python module that interfaces to a PostgreSQL database.
It embeds the PostgreSQL query library to allow easy use of the powerful
PostgreSQL features from a Python script.

PyGreSQL is developed and tested on a NetBSD system, but it should also
run on most other platforms where PostgreSQL and Python is running.
It is based on the PyGres95 code written by Pascal Andre (andre@chimay.via.ecp.fr).
D'Arcy (darcy@druid.net) renamed it to PyGreSQL starting with
version 2.0 and serves as the "BDFL" of PyGreSQL.

The current version PyGreSQL 4.0 needs PostgreSQL 7.2 and Python 2.3 or above.


Where to get ... ?
==================

Home sites of the different packages
------------------------------------
**Python**:
  http://www.python.org

**PostgreSQL**:
  http://www.postgresql.org

**PyGreSQL**:
  http://www.pygresql.org

Download PyGreSQL here
----------------------
The **released version of the source code** is available at
  * ftp://ftp.pygresql.org/pub/distrib/PyGreSQL.tgz
You can also check the latest **pre-release version** at
  * ftp://ftp.pygresql.org/pub/distrib/PyGreSQL-beta.tgz
A **Linux RPM** can be picked up from
  * ftp://ftp.pygresql.org/pub/distrib/pygresql.i386.rpm
A **NetBSD package** is available in their pkgsrc collection
  * ftp://ftp.netbsd.org/pub/NetBSD/packages/pkgsrc/databases/py-postgresql/README.html
A **FreeBSD package** is available in their ports collection
  * http://www.freebsd.org/cgi/cvsweb.cgi/ports/databases/py-PyGreSQL/
A **Win32 package** for various Python versions is available at
  * ftp://ftp.pygresql.org/pub/distrib/PyGreSQL-4.0.win32-py2.3.exe
  * ftp://ftp.pygresql.org/pub/distrib/PyGreSQL-4.0.win32-py2.4.exe
  * ftp://ftp.pygresql.org/pub/distrib/PyGreSQL-4.0.win32-py2.5.exe
  * ftp://ftp.pygresql.org/pub/distrib/PyGreSQL-4.0.win32-py2.6.exe
You can also find PyGreSQL on the **Python Package Index** at
 * http://pypi.python.org/pypi/PyGreSQL/


Distribution files
==================

========== =
pgmodule.c the C Python module (_pg)
pg.py      the "classic" PyGreSQL module
pgdb.py    DB-SIG DB-API 2.0 compliant API wrapper for PygreSQL
docs/      documentation directory

           Contains: readme.txt, announce.txt, install.txt,
           changelog.txt, future.txt, pg.txt and pgdb.txt.

           All text files are in ReST format, so HTML versions
           can be easily created with buildhtml.py from docutils.
tutorial/  demos directory

           Contains: basics.py, syscat.py, advanced.py and func.py.

           The samples here have been taken from the
           PostgreSQL manual and were used for module testing.
           They demonstrate some PostgreSQL features.
========== =


Installation
============
You will find the installing instructions in
`install.txt <install.html>`_.


Information and support
=======================

For general information
-----------------------
**Python**:
  http://www.python.org

**PostgreSQL**:
  http://www.postgresql.org

**PyGreSQL**:
  http://www.pygresql.org

For support
-----------
**Python**:
  see http://www.python.org/community/

**PostgreSQL**:
  see http://www.postgresql.org/support/

**PyGreSQL**:
  Contact the PyGreSQL mailing list
  concerning PyGreSQL 2.0 and up.

  If you would like to proposes changes, please join the
  PyGreSQL mailing list and send context diffs there.

  See http://mailman.vex.net/mailman/listinfo/pygresql
  to join the mailing list.

Please note that messages to individual developers will generally not be
answered directly.  All questions, comments and code changes must be
submitted to the mailing list for peer review and archiving.

PyGreSQL programming information
--------------------------------
You may either choose to use the "classic" PyGreSQL interface
provided by the `pg` module or else the newer DB-API 2.0
compliant interface provided by the `pgdb` module.

`DB-API 2.0 <http://www.python.org/dev/peps/pep-0249/>`_
(Python Database API Specification v2.0)
is a specification for connecting to databases (not only PostGreSQL)
from Python that has been developed by the Python DB-SIG in 1999.

The programming information is available in the files
`pg.txt <pg.html>`_ and `pgdb.txt <pgdb.html>`_.

Note that PyGreSQL is not thread-safe on the connection level. Therefore
we recommend using `DBUtils <http://www.webwareforpython.org/DBUtils>`
for multi-threaded environments, which supports both PyGreSQL interfaces.


ChangeLog and Future
====================
The ChangeLog with past changes is in the file
`changelog.txt <changelog.html>`_.

A to do list and wish list is in the file
`future.txt <future.html>`_.
