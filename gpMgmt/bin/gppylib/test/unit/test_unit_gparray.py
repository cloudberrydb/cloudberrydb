#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Line too long - pylint: disable=C0301
#
# Copyright (c) Greenplum Inc 2008. All Rights Reserved. 
#

""" Unittesting for gplog module
"""
import unittest2 as unittest

from gppylib.gparray import GpArray, GpDB, createSegmentRows
from gppylib import gplog

logger = gplog.get_unittest_logger()

class MyTestCase(unittest.TestCase):
    def test_spreadmirror_layout(self):
        """ Basic spread mirroring """
        mirror_type = 'spread'
        interface_list = [1]
        primary_portbase = 5000
        mirror_portbase = 6000
        primary_replication_portbase = 7000
        mirror_replication_portbase = 8000
        hostlist = ['host1']
        primary_list = ['/db1']
        mirror_list = ['/mir1']
        dir_prefix = 'gpseg'
        
        #need to have enough hosts otherwise we get exceptions
        with self.assertRaises(Exception):
            createSegmentRows(hostlist, interface_list, primary_list, primary_portbase, mirror_type, 
                              mirror_list, mirror_portbase, dir_prefix, primary_replication_portbase, mirror_replication_portbase)
        primary_list.append('/db2')
        mirror_list.append('/mir2')
        with self.assertRaises(Exception):
            createSegmentRows(hostlist, interface_list, primary_list, primary_portbase, mirror_type, 
                              mirror_list, mirror_portbase, dir_prefix, primary_replication_portbase, mirror_replication_portbase)
        hostlist.append('host2')
        with self.assertRaises(Exception):
            createSegmentRows(hostlist, interface_list, primary_list, primary_portbase, mirror_type, 
                              mirror_list, mirror_portbase, dir_prefix, primary_replication_portbase, mirror_replication_portbase)
        
        #now we have enough
        hostlist.append('host3')
        self.mirrorlayout_test(hostlist, interface_list, primary_list, primary_portbase, mirror_type,
                               mirror_list, mirror_portbase, dir_prefix, primary_replication_portbase, mirror_replication_portbase)
        
        
        #enough
        hostlist = ['host1', 'host2', 'host3']
        primary_list = ['/db1', '/db2']
        mirror_list = ['/mir1', '/mir2']
        self.mirrorlayout_test(hostlist, interface_list, primary_list, primary_portbase, mirror_type,
                               mirror_list, mirror_portbase, dir_prefix, primary_replication_portbase, mirror_replication_portbase)

        #typical thumper
        hostlist = ['sdw1', 'sdw2', 'sdw3', 'sdw4', 'sdw5']
        primary_list = ['/dbfast1', '/dbfast2', '/dbfast3', '/dbfast4']
        mirror_list = ['/dbfast1/mirror', '/dbfast2/mirror', '/dbfast3/mirror', '/dbfast4/mirror']
        self.mirrorlayout_test(hostlist, interface_list, primary_list, primary_portbase, mirror_type,
                               mirror_list, mirror_portbase, dir_prefix, primary_replication_portbase, mirror_replication_portbase)

        #typical Thor
        hostlist = ['sdw1', 'sdw2', 'sdw3', 'sdw4', 'sdw5', 'sdw6', 'sdw7', 'sdw8', 'sdw9']
        primary_list = ['/dbfast1', '/dbfast2', '/dbfast3', '/dbfast4', '/dbfast5', '/dbfast6', '/dbfast7', '/dbfast8']
        mirror_list = ['/dbfast1/mirror', '/dbfast2/mirror', '/dbfast3/mirror', '/dbfast4/mirror',
                       '/dbfast5/mirror', '/dbfast6/mirror', '/dbfast7/mirror', '/dbfast8/mirror']
        self.mirrorlayout_test(hostlist, interface_list, primary_list, primary_portbase, mirror_type,
                               mirror_list, mirror_portbase, dir_prefix, primary_replication_portbase, mirror_replication_portbase)

    def test_groupmirror_layout(self):
        """ Basic group mirroring """
        mirror_type = 'grouped'
        primary_portbase = 5000
        mirror_portbase = 6000
        interface_list = [1]
        primary_replication_portbase = 7000
        mirror_replication_portbase = 8000
        hostlist = ['host1']
        primary_list = ['/db1']
        mirror_list = ['/mir1']
        dir_prefix = 'gpseg'
        
        #not enough
        with self.assertRaises(Exception):
            createSegmentRows(hostlist, interface_list, primary_list, primary_portbase, mirror_type,
                              mirror_list, mirror_portbase, dir_prefix, primary_replication_portbase, mirror_replication_portbase)

        primary_list.append('/db2')
        mirror_list.append('/mir2')
        with self.assertRaises(Exception):
            createSegmentRows(hostlist, interface_list, primary_list, primary_portbase, mirror_type,
                              mirror_list, mirror_portbase, dir_prefix, primary_replication_portbase, mirror_replication_portbase)
        
        #enough
        hostlist = ['host1', 'host2']
        primary_list = ['/db1', '/db2']
        mirror_list = ['/mir1', '/mir2']
        
         #typical thumper
        hostlist = ['sdw1', 'sdw2', 'sdw3', 'sdw4', 'sdw5']
        primary_list = ['/dbfast1', '/dbfast2', '/dbfast3', '/dbfast4']
        mirror_list = ['/dbfast1/mirror', '/dbfast2/mirror', '/dbfast3/mirror', '/dbfast4/mirror']
        self.mirrorlayout_test(hostlist, interface_list, primary_list, primary_portbase, mirror_type,
                               mirror_list, mirror_portbase, dir_prefix, primary_replication_portbase, mirror_replication_portbase)

        #typical Thor
        hostlist = ['sdw1', 'sdw2', 'sdw3', 'sdw4', 'sdw5', 'sdw6', 'sdw7', 'sdw8', 'sdw9']
        primary_list = ['/dbfast1', '/dbfast2', '/dbfast3', '/dbfast4', '/dbfast5', '/dbfast6', '/dbfast7', '/dbfast8']
        mirror_list = ['/dbfast1/mirror', '/dbfast2/mirror', '/dbfast3/mirror', '/dbfast4/mirror',
                       '/dbfast5/mirror', '/dbfast6/mirror', '/dbfast7/mirror', '/dbfast8/mirror']
        self.mirrorlayout_test(hostlist, interface_list, primary_list, primary_portbase, mirror_type,
                               mirror_list, mirror_portbase, dir_prefix, primary_replication_portbase, mirror_replication_portbase)        


#------------------------------- non-test helper --------------------------------
    def mirrorlayout_test(self, hostlist, interface_list, primary_list, primary_portbase, mirror_type, 
                          mirror_list, mirror_portbase, dir_prefix, primary_replication_portbase, mirror_replication_portbase):
        master = GpDB(content = -1,
                    preferred_role = 'p',
                    dbid = 0,
                    role = 'p',
                    mode = 's',
                    status = 'u',
                    hostname = 'masterhost',
                    address = 'masterhost-1',
                    port = 5432,
                    datadir = '/masterdir',
                    replicationPort = 5433)
        allrows = []
        allrows.append(master)                 
        rows =  createSegmentRows(hostlist, interface_list, primary_list, primary_portbase, mirror_type,
                                  mirror_list, mirror_portbase, dir_prefix, primary_replication_portbase, mirror_replication_portbase)
        
        
        for row in rows:
            newrow = GpDB(content = row.content, 
                          preferred_role = 'p' if convert_bool(row.isprimary) else 'm', 
                          dbid = row.dbid,
                          role = 'p' if convert_bool(row.isprimary) else 'm',
                          mode = 's', 
                          status = 'u', 
                          hostname = row.host, 
                          address = row.address, 
                          port = row.port, 
                          datadir = row.fulldir, 
                          replicationPort = row.prPort) 
            allrows.append(newrow)
        
        gparray = GpArray(allrows)
                
        self._validate_array(gparray)
    
        
    def _validate_array(self, gparray): 
        portdict = {}
        lastport = 0
        for seg in gparray.segments:
            prim = seg.primaryDB            
            mir = seg.mirrorDBs[0]
            self.assertNotEqual(prim.hostname, mir.hostname)
            if prim.port not in portdict:
                portdict[prim.port] = 1
            else:
                portdict[prim.port] = 1 + portdict[prim.port]
            lastport = prim.port
            
        expected_count = portdict[lastport]
            
        for count in portdict.values():
            self.assertEquals(expected_count, count)
            
    
def convert_bool(val):
    if val == 't':
        return True
    else:
        return False   

#------------------------------- Mainline --------------------------------
if __name__ == '__main__':
    unittest.main()
