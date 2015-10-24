#!/usr/bin/env python
#
# Copyright (c) Greenplum Inc 2008. All Rights Reserved.
#
# Greenplum SAN related utility functions

import os
from gppylib.commands import unix

class SanCmds():
    INQ=None
    NAVISECCLI=None
    POWERMT=None
    STAT=None
    GP_MOUNT_AGENT=None
    MOUNT=None
    
    def __init__(self):
        gphome = os.getenv('GPHOME', None) + '/bin/'
        
        try:
            self.GP_MOUNT_AGENT = unix.findCmdInPath('gp_mount_agent', [gphome], False)
        except:
            pass
        try:
            self.POWERMT = unix.findCmdInPath('powermt', printError=False)
        except:
            pass
        try:
            self.NAVISECCLI = unix.findCmdInPath('naviseccli', printError=False)
        except:
            pass
        try:
            self.STAT = unix.findCmdInPath('stat', printError=False)
        except:
            pass
        try:
            self.INQ = unix.findCmdInPath('inq', printError=False)
        except:
            pass
        try:
            self.MOUNT = unix.findCmdInPath('mount', printError=False)
        except:
            pass
    
SAN_CMDS=SanCmds()
