#!/usr/bin/env python
#
# Copyright (c) Greenplum Inc 2008. All Rights Reserved. 
#

""" Unittesting for pg8000 module
"""
import logging
import unittest

try: 
    from pg8000 import *
except ImportError, e:
    sys.exit('Error: unable to import module: ' + str(e))



from gppylib import gplog
from gppylib.db.dbconn import *
from gppylib.db.test import skipIfDatabaseDown

logger=gplog.get_unittest_logger()
gplog.enable_verbose_logging()


@skipIfDatabaseDown()
class pg8000TestCase(unittest.TestCase):

    def test_connect(self):
        conn=None        
        try:
            #def connect(user, host=None, unix_sock=None, port=5432, database=None, password=None, socket_timeout=60, ssl=False, options=None):
            dburl = DbURL()        
            self.failUnlessRaises(TypeError,DBAPI.connect)        
            self.failUnlessRaises(ProgrammingError,DBAPI.connect,dburl.pguser)        
            conn=DBAPI.connect(dburl.pguser,host=dburl.pghost,database=dburl.pgdb)
        finally:
            if conn: conn.close()
        
    def test_nonexist_objects(self):
        conn=None
        try:
            dburl=DbURL()
            conn=DBAPI.connect(dburl.pguser,host=dburl.pghost,database=dburl.pgdb)
            curs=conn.cursor()
            self.assertRaises(ProgrammingError,curs.execute,"DROP TABLE notable")
        finally:
            if conn: conn.close()
         
    def test_datatypes(self):
        conn=None
        try:
            
            dburl=DbURL()
            conn=DBAPI.connect(dburl.pguser,host=dburl.pghost,database=dburl.pgdb)
            curs=conn.cursor()
            curs.execute("CREATE TABLE tab1 ( a int )")
            self.assertRaises(ProgrammingError,curs.execute,"INSERT INTO tqb1 VALUES 1")
            self.assertRaises(ProgrammingError,curs.execute,"DROP TABLE tab1")
            self.assertRaises(ProgrammingError,conn.commit)
        finally:
            if conn: conn.close()
    
            

#------------------------------- non-test helper --------------------------------

#------------------------------- Mainline --------------------------------
if __name__ == '__main__':
    unittest.main()

