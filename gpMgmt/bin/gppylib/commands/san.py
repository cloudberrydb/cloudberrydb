#!/usr/bin/env python
#
# Copyright (c) Greenplum Inc 2008. All Rights Reserved.
#
# Greenplum SAN related utility commands

from gppylib.commands.base import *
from gppylib.util.san_utils import SAN_CMDS

#----------mount ---------------
class Mount(Command):
    """Retrieves mount points."""
    
    def __init__(self, name, devfirst=True, ctxt=LOCAL, remoteHost=None):
        cmdStr = SAN_CMDS.MOUNT
        self.devfirst = devfirst
        print cmdStr
        Command.__init__(self, name, cmdStr, ctxt, remoteHost)
        
    @staticmethod
    def remote(name, hostname, devfirst=True):
        cmd = Mount(name, devfirst, ctxt=REMOTE, remoteHost=hostname)
        cmd.run(validateAfter=True)
        return cmd
    
    def get_mount_points(self):
        mps = []
        out = self.get_results().stdout.strip()
        for l in out.split('\n'):
            x = l.strip().split()
            if self.devfirst:
                thismp = (x[0], x[2])
            else:
                thismp = (x[2], x[0])
            mps.append(thismp)
        return mps
    
#----------inq ---------------
class Inq(Command):
    def __init__(self, name, ctxt=LOCAL, remoteHost=None):
        cmdStr = '%s -clar_wwn -no_dots' % SAN_CMDS.INQ
        Command.__init__(self, name, cmdStr, ctxt, remoteHost)

    @staticmethod
    def remote(name, hostname):
        cmd = Inq(name, ctxt=REMOTE, remoteHost=hostname)
        cmd.run(validateAfter=True)
        return cmd
    
#----------gp_mount_agent ---------------
class GpMountAgentStorageGroup(Command):
    def __init__(self, name, ctxt=LOCAL, remoteHost=None):
        cmdStr = '%s --storagegroup %s' % (SAN_CMDS.GP_MOUNT_AGENT, SAN_CMDS.POWERMT)
        Command.__init__(self, name, cmdStr, ctxt, remoteHost)
    
    @staticmethod
    def remote(name, hostname):
        cmd = GpMountAgentStorageGroup(name, ctxt=REMOTE, remoteHost=hostname)
        cmd.run(validateAfter=True)
        return cmd
    
