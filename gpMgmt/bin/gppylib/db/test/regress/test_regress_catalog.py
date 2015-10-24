#!/usr/bin/env python
#
# Copyright (c) Greenplum Inc 2008. All Rights Reserved. 
#
# Unit Testing of catalog module.
#

import unittest2 as unittest



from gppylib import gplog
from gppylib.db import dbconn
from gppylib.db import catalog
from gppylib.db.test import skipIfDatabaseDown


logger=gplog.get_default_logger()


@skipIfDatabaseDown()
class catalogTestCase(unittest.TestCase):
    
    def setUp(self):
        self.dburl=dbconn.DbURL()
        self.conn = dbconn.connect(self.dburl)
        
    
    def tearDown(self):
        self.conn.close()
        pass

    def test_vacuumcatalog(self):
        logger.info("test_vacuumcatalog")
        catalog.vacuum_catalog(self.dburl,self.conn)
        catalog.vacuum_catalog(self.dburl,self.conn,full=True)
    

#------------------------------- Helper --------------------------------
    def validate_gpid_table(self,orig_gpid_table,test_gpid_table):
        self.assertEquals(orig_gpid_table.gpname,test_gpid_table.gpname)
        self.assertEquals(orig_gpid_table.numsegments ,test_gpid_table.numsegments)
        self.assertEquals(orig_gpid_table.dbid ,test_gpid_table.dbid)
        self.assertEquals(orig_gpid_table.content ,test_gpid_table.content)
    
    def validate_versionatinitdb_table(self,orig_table,test_table):
        self.assertEquals(orig_table.schemaversion,test_table.schemaversion)
        self.assertEquals(orig_table.productversion,orig_table.productversion)
        



#------------------------------- Mainline --------------------------------
if __name__ == '__main__':
    unittest.main()    
