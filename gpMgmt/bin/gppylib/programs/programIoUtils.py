#
# Copyright (c) Greenplum Inc 2010. All Rights Reserved.
#
from gppylib import gparray, gplog, pgconf, userinput

logger = gplog.get_default_logger()

def appendSegmentInfoForOutput(instanceType, gpArray, seg, tableLogger):
    """
    @param tableLogger an instance of utils.TableLogger
    """

    tableLogger.info(['%s instance host' % instanceType, "= " + seg.getSegmentHostName()])
    tableLogger.info(['%s instance address' % instanceType, "= " + seg.getSegmentAddress()])
    tableLogger.info(['%s instance directory' % instanceType, "= " + seg.getSegmentDataDirectory()])
    tableLogger.info(['%s instance port' % instanceType, "= " + str(seg.getSegmentPort())])
    tableLogger.info(['%s instance replication port' % instanceType, "= " + str(seg.getSegmentReplicationPort())])

    for fs in gpArray.getFilespaces(False):
        path = seg.getSegmentFilespaces()[fs.getOid()]
        tableLogger.info(['%s instance %s directory' % (instanceType, fs.getName()), "= " + path])



