#!/usr/bin/env python
#
# Copyright (c) Greenplum Inc 2013. All Rights Reserved. 
#
# Unittests for gpmfr.py.
#

import os
import re
import signal
import sys
import threading
import time
import unittest

from gppylib.mainUtils import ProgramArgumentValidationException
sys.path.append(os.environ["GPHOME"] + "/bin/lib")
import gpmfr

class TestGpMfr(unittest.TestCase):
    def setUp(self):
        p = gpmfr.mfr_parser()
        opt, args = p.parse_args(["--list"], None)
        self.mfr = gpmfr.GpMfr(opt, args)
        pass

    def tearDown(self):
        pass

    def test_validateCmdLine(self):
        """
        Ensure validations on command line options are working correctly.
        """
        p = gpmfr.mfr_parser()
        # At the most one of --list, --delete, --replicate, --recover options
        # may be specified.
        opt, args = p.parse_args(["--list",
                                  "--replicate='2012-May-11 21:00:00'"], None)
        try:
            gpmfr.GpMfr(opt, args)
            self.fail("At the most one operation can be specified")
        except ProgramArgumentValidationException:
            pass
        opt, args = p.parse_args(["--delete='1999-June-12'",
                                  "--replicate='1234-February-09'"],
                                 None)
        try:
            gpmfr.GpMfr(opt, args)
            self.fail("At the most one operation can be specified")
        except ProgramArgumentValidationException:
            pass

        # Timestamp argument must conform to YYYYMMDD format.  Validate this.
        opt, args = p.parse_args(["--replicate", "1999-February-12 11:55:55"],
                                 None)
        try:
            gpmfr.GpMfr(opt, args)
        except:
            self.fail("Valid timestamp not accepted.")
        opt, args = p.parse_args(["--recover", "2000-February-30 25:67:12"],
                                 None)
        try:
            gpmfr.GpMfr(opt, args)
            self.fail("Invalid timestamp accepted.")
        except ProgramArgumentValidationException:
            pass
        opt,args = p.parse_args(["--delete", "1999-May-31 55:55:78"], None)
        try:
            gpmfr.GpMfr(opt, args)
            self.fail("Invalid timestamp accepted.")
        except ProgramArgumentValidationException:
            pass

        opt,args = p.parse_args(["--delete", "1999-May31 13:55:00"], None)
        try:
            gpmfr.GpMfr(opt, args)
            self.fail("Invalid timestamp accepted.")
        except ProgramArgumentValidationException:
            pass

    def test_backupDirsOnDD(self):
        input1 = """
20130123:14:19:58|ddboost-[DEBUG]:-Libraries were loaded successfully
20130123:14:19:58|ddboost-[INFO]:-opening LB on /home/asim/DDBOOST_CONFIG

                NAME                       				MODE	SIZE(bytes)
===============================================================================
20121108                                          			D755	901350
20121113                                          			D755	727
20130108                                          			D755	905
20130111                                          			D755	181
5                                                 			600	10737418240
200                                               			600	10737418240
3                                                 			600	10737418240
1                                                 			600	10737418240
4                                                 			600	10737418240
2                                                 			600	10737418240
401                                               			600	1073741824
20130122                                          			D755	249115
20130123                                          			D755	112545
"""
        backups = self.mfr.backupDirsOnDD(input1.splitlines())
        self.assertEquals(backups, ["20121108", "20121113", "20130108",
                                    "20130111", "20130122", "20130123"])

    def test_parseParseBackupListing(self):
        input = """
20130123:14:19:58|ddboost-[DEBUG]:-Libraries were loaded successfully
20130123:14:19:58|ddboost-[INFO]:-opening LB on /home/asim/DDBOOST_CONFIG

                NAME                       				MODE	SIZE(bytes)
===============================================================================
gp_dump_0_74_20130125191202.gz                          600     356252544
gp_dump_0_67_20130125191202.gz                          600     356123065
gp_dump_0_65_20130125191202.gz                          600     356109827
gp_segment_config_files_0_6_20130122120556.tar    			600	30720
gp_dump_0_41_20130125191202.gz                          600     356100496
gp_dump_0_69_20130122120055.gz                    			600	356013486
gp_cdatabase_1_1_20130122120055                   			777	118
gp_dump_0_96_20130122120055.gz                    			600	356082487
gp_dump_0_31_20130122120055.gz                    			600	356017439
gp_dump_1_1_20130122120055_post_data.gz           			600	190
gp_dump_0_75_20130122120519.gz                    			600	356200243
gp_dump_0_71_20130122120519.gz                    			600	355925432
gp_dump_0_50_20130122120519.gz                    			600	356091218
gp_dump_0_68_20130122120519.gz                    			600	355995148
gp_cdatabase_1_1_20130122120519                   			777	118
gp_dump_1_1_20130122120519_post_data.gz           			600	190
gp_global_1_1_20130122120519                      			600	786
gp_master_config_files_20130122120556.tar         			600	30720
gp_segment_config_files_0_2_20130122120556.tar    			600	30720
gp_segment_config_files_0_7_20130122120556.tar    			600	30720
        """
        bkpSets = self.mfr.parseBackupListing(input.splitlines())
        self.assertEquals(len(bkpSets), 4)
        self.assertEquals(bkpSets[0].backupDate, "20130122")
        self.assertEquals(bkpSets[0].backupTime, "120055")
        self.assertEquals(bkpSets[3].backupDate, "20130125")
        self.assertEquals(bkpSets[3].backupTime, "191202")
        self.assertEquals(len(bkpSets[0].backupFiles), 5)
        self.assertEquals(len(bkpSets[3].backupFiles), 4)

    def test_backupSet(self):
        b1 = gpmfr.BackupSet("19091122", "111122")
        b2 = gpmfr.BackupSet("19091122", "111122")
        self.assertEquals(b1, b2)
        f1 = gpmfr.BackupFile("gp_global_1_1_19091122111122",
                                 "600", 23411232)
        f2 = gpmfr.BackupFile("gp_dump_0_75_19091122111122",
                                 "777", 981239823)
        b1.addFile(f1)
        b2.addFile(f2)
        self.assertFalse(b1 == b2)
        b1.addFile(f2)
        b2.addFile(f1)
        self.assertEquals(len(b1.backupFiles), len(b2.backupFiles))
        self.assertEquals(b1.userFormatString(b1.backupDate+b1.backupTime),
                          b2.userFormatString(b2.backupDate+b2.backupTime))

        b3 = gpmfr.BackupSet("19091122", "120000")
        self.assertTrue(b3 > b1)
        self.assertTrue(b2 < b3)

        # Unless validateWithGpdb() is called, isValid must be None.
        self.assertEqual(b1.isValid, None)
        self.assertEqual(b3.isValid, None)

        userStr = b1.userFormatString(b1.backupDate+b1.backupTime)
        self.assertEquals(userStr, "1909-November-22 11:11:22")
        self.assertTrue(
            gpmfr.BackupSet.isValidTimestamp("2000-May-31 09:15:00"))
        self.assertFalse(
            gpmfr.BackupSet.isValidTimestamp("2000-May-31 09:15AM"))

    def test_backupWorker(self):
        # FIXME: This test depends on availability of a specific backup on
        # local/remote DD system.
        local = gpmfr.DDSystem("local")
        remote = gpmfr.DDSystem("remote")
        bf = gpmfr.BackupFile("gp_dump_0_3_20130131140403",
                              "600", long(4689531688))
        bf.fullPath = "db_dumps/20130131/" + bf.name
        ce = threading.Event()
        # Test regular file transfer
        bw1 = gpmfr.BackupWorker(bf, local, remote, ce)
        bw1.start()
        try:
            while bw1.state == bw1.INIT:
                time.sleep(0.01)
            while bw1.bytesSent < bf.size:
                if bw1.state == bw1.TRANSFER_INPROGRESS:
                    self.assertTrue(bw1.bytesSent > 0)
                percent = 100*(bf.size - bw1.bytesSent)/bf.size
                self.assertTrue(
                    bw1.state in (bw1.DDSTARTED, bw1.TRANSFER_INPROGRESS))
                time.sleep(0.1)
        except KeyboardInterrupt:
            ce.set()
            print "Cancel event set"
        bw1.join()
        if not ce.isSet():
            self.assertEquals(bw1.state, bw1.TRANSFER_COMPLETE)
        else:
            self.assertEquals(bw1.state, bw1.TRANSFER_CANCELED)

        # Test transfer cancel
        ce.clear()
        bw2 = gpmfr.BackupWorker(bf, remote, local, ce)
        bw2.start()
        while bw2.state == bw2.INIT:
            time.sleep(0.2)
        while bw2.state != bw2.TRANSFER_INPROGRESS:
            time.sleep(0.01)
        ce.set()
        bw2.proc.send_signal(signal.SIGINT)
        while bw2.state == bw2.TRANSFER_INPROGRESS:
            time.sleep(0.2)
        self.assertEquals(bw2.state, bw2.TRANSFER_CANCELED)
        
        
if __name__ == "__main__":
    unittest.main()
