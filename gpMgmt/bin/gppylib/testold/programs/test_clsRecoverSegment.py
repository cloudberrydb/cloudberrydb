#!/usr/bin/env python
#
#
from gppylib.testold.testDriver import TestDriver
from gppylib.programs.clsRecoverSegment import *
from gppylib.operations.buildMirrorSegments import *
from gppylib.mainUtils import *
from gppylib.testold.testUtils import *
from gppylib.system import fileSystemImplTest, fileSystemInterface
from gppylib.gplog import get_default_logger

programName = sys.argv[0]
parserFn = GpRecoverSegmentProgram.createParser
commandFn = GpRecoverSegmentProgram.createProgram

driver = TestDriver()
logger = get_default_logger()

###############
#
# Now the tests:
#
####################################
#
#
def testPortAssigner(args):
    driver.initTwoSegmentOneFailedMirrorConfiguration()
    confProvider = configInterface.getConfigurationProvider().initializeProvider(5432)
    gpArray = confProvider.loadSystemConfig(useUtilityMode=False)

    masterSeg = [seg for seg in gpArray.getDbList() if not seg.isSegmentQE()][0]
    masterSeg.setSegmentPort(35001) # to make sure master is avoided when assigning ports
    masterSeg.setSegmentHostName("first-host") # to make sure master is avoided when assigning ports

    portAssigner = PortAssigner(gpArray)

    testOutput("")
    for (host, replPortOrRegular) in args:
        testOutput( portAssigner.findAndReservePort(replPortOrRegular, host, host))
    testOutput("")

simple_test("testPortAssigner_0", testPortAssigner, [
        ("first-host", True),
        ("first-host", True),
        ("first-host", True),
        ("first-host", False),
        ("first-host", True),
        ("first-host", False),
        ],
"""
35002
35003
35004
30001
35005
30002
"""
)
simple_test("testPortAssigner_0", testPortAssigner, [
        ("first-host", True),
        ("second-host", True),
        ("third-host", True),
        ("fourth-host", True),
        ("fifth-host", True),
        ("sixth-host", True),
        ("fourth-host", True),
        ("fifth-host", True),
        ("sixth-host", True),
        ],
"""
35002
35003
35001
35001
35001
35001
35002
35002
35002
"""
)


#
# Test that recovering from a config file that tells us to recover something that can't be recovered fails
#
def testRecoverFromConfigFileChecksWhetherRecoveryIsPossible(args):
    driver.initTwoSegmentOneFailedMirrorConfiguration()
    confProvider = configInterface.getConfigurationProvider().initializeProvider(5432)
    gpArray = confProvider.loadSystemConfig(useUtilityMode=False)

    seg = recoverFrom = None
    if args[0] == 0:
        seg = [seg for seg in gpArray.getSegDbList() if seg.isSegmentUp()][0]
    elif args[0] == 1:
        seg = [seg for seg in gpArray.getSegDbList() if seg.isSegmentPrimary(current_role=True)][0]
        seg.setSegmentStatus(gparray.STATUS_DOWN)
    elif args[0] == 2:
        seg = [seg for seg in gpArray.getDbList() if not seg.isSegmentQE()][0]
        recoverFrom = seg # dummy just to get further along

    if recoverFrom is None:
        recoverFrom = gpArray.getDbIdToPeerMap()[seg.getSegmentDbId()]
    try:
        toBuild = [GpMirrorToBuild(seg, recoverFrom, None, False)]
        mirrorBuilder = GpMirrorListToBuild(toBuild, pool=None, quiet=False, parallelDegree=1)
    except Exception, e:
        testOutput("Validation Error: %s" % e)

simple_test("testRecoverFromConfigFileChecksWhetherRecoveryIsPossible_0", testRecoverFromConfigFileChecksWhetherRecoveryIsPossible, [0],
"""Validation Error: Segment to recover from for content 0 is not a primary"""
)
simple_test("testRecoverFromConfigFileChecksWhetherRecoveryIsPossible_1", testRecoverFromConfigFileChecksWhetherRecoveryIsPossible, [1],
"""Validation Error: Segment to recover from for content 0 is not a primary"""
)
simple_test("testRecoverFromConfigFileChecksWhetherRecoveryIsPossible_2", testRecoverFromConfigFileChecksWhetherRecoveryIsPossible, [2],
"""Validation Error: Segment to recover from for content -1 is not a correct segment (it is a master or standby master)"""
)

def testRecoveryWarningsWhenRecoveringToSameHost(args):
    """
    Test the function that checks for recovering so that a mirror and primary are on the same machine
    """
    driver.initTwoSegmentOneFailedMirrorConfiguration()
    confProvider = configInterface.getConfigurationProvider().initializeProvider(5432)
    gpArray = confProvider.loadSystemConfig(useUtilityMode=False)

    toBuild = []
    if args[0] == 0:
        pass
    elif args[0] == 1:
        seg = [seg for seg in gpArray.getSegDbList() if not seg.isSegmentUp()][0]
        recoverFrom = gpArray.getDbIdToPeerMap()[seg.getSegmentDbId()]

        failoverTarget = seg.copy()
        failoverTarget.setSegmentAddress("different-address")
        failoverTarget.setSegmentHostName(recoverFrom.getSegmentHostName())

        toBuild.append(GpMirrorToBuild(seg, recoverFrom, failoverTarget, False))
    else: raise Exception("invalid test option")

    mirrorBuilder = GpMirrorListToBuild(toBuild, pool=None, quiet=False, parallelDegree=1)

    program = GpRecoverSegmentProgram({})
    warnings = program._getRecoveryWarnings(mirrorBuilder)
    if warnings:
        for w in warnings:
            testOutput(w)
    else: testOutput("No warnings")

simple_test("testRecoveryWarningsWhenRecoveringToSameHost_0", testRecoveryWarningsWhenRecoveringToSameHost, [0],
"""No warnings"""
)
simple_test("testRecoveryWarningsWhenRecoveringToSameHost_1", testRecoveryWarningsWhenRecoveringToSameHost, [1],
"""Segment is being recovered to the same host as its primary: primary second-host:/second/datadirpathdbfast2/gp1    failover target: different-address:/datadirpathdbfast4/gp1"""
)



# All done tests
printTestResults()
