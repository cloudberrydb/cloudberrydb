#!/usr/bin/env python

import os

GPMGMT_FAULT_POINT = 'GPMGMT_FAULT_POINT'

def inject_fault(fault_point):
    if GPMGMT_FAULT_POINT in os.environ and fault_point == os.environ[GPMGMT_FAULT_POINT]:
        raise Exception('Fault Injection %s' % os.environ[GPMGMT_FAULT_POINT]) 
