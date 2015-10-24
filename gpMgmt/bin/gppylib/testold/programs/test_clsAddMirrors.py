#!/usr/bin/env python
#
#
from gppylib.testold.testDriver import TestDriver
from gppylib.programs.clsAddMirrors import *
from gppylib.operations.buildMirrorSegments import *
from gppylib.mainUtils import *
from gppylib.testold.testUtils import *
from gppylib.system import fileSystemImplTest, fileSystemInterface
from gppylib.gplog import get_default_logger

programName = sys.argv[0]
parserFn = GpAddMirrorsProgram.createParser
commandFn = GpAddMirrorsProgram.createProgram

driver = TestDriver()
logger = get_default_logger()

###############
#
# Now the tests:
#
####################################
#
#
#
# Test that spread mirror assigns ports and directories correctly
#
def testSpreadMirror(args):
    driver.initThreeHostMultiHomeNoMirrors()
    confProvider = configInterface.getConfigurationProvider().initializeProvider(5432)
    gpArray = confProvider.loadSystemConfig(useUtilityMode=False)

    calc = GpMirrorBuildCalculator(gpArray, 1000, [ "/data/m1", "/data/m2"], [{},{}])
    calc.getSpreadMirrors()

    GpMirrorListToBuild([], None, False, 1).checkForPortAndDirectoryConflicts(gpArray)

    testOutput("")
    testOutputGpArray(gpArray)
    testOutput("")
    
simple_test("testSpreadMirror", testSpreadMirror, [],
"""
dbid  | content  | role  | preferred_role  | mode  | status  | hostname     | address        | port   | datadir                         | replication_port
1     | -1       | p     | p               | s     | u       | master-host  | primary-host   | 5432   | /datadirpathdbmaster/gp-1       | None
2     | 0        | p     | p               | r     | u       | first-host   | first-host-1   | 50001  | /first/datadirpathdbfast1/gp0   | 53001
3     | 1        | p     | p               | r     | u       | first-host   | first-host-2   | 50002  | /first/datadirpathdbfast2/gp1   | 53002
4     | 2        | p     | p               | r     | u       | second-host  | second-host-1  | 50001  | /second/datadirpathdbfast1/gp2  | 53001
5     | 3        | p     | p               | r     | u       | second-host  | second-host-2  | 50002  | /second/datadirpathdbfast2/gp3  | 53002
6     | 4        | p     | p               | r     | u       | third-host   | third-host-1   | 50001  | /third/datadirpathdbfast2/gp4   | 53001
7     | 5        | p     | p               | r     | u       | third-host   | third-host-2   | 50002  | /third/datadirpathdbfast2/gp5   | 53002
8     | 0        | m     | m               | r     | u       | second-host  | second-host-1  | 51001  | /data/m1                        | 52001
9     | 1        | m     | m               | r     | u       | third-host   | third-host-1   | 51001  | /data/m1                        | 52001
10    | 2        | m     | m               | r     | u       | third-host   | third-host-2   | 51002  | /data/m2                        | 52002
11    | 3        | m     | m               | r     | u       | first-host   | first-host-1   | 51001  | /data/m1                        | 52001
12    | 4        | m     | m               | r     | u       | first-host   | first-host-2   | 51002  | /data/m2                        | 52002
13    | 5        | m     | m               | r     | u       | second-host  | second-host-2  | 51002  | /data/m2                        | 52002
"""
)

#
# Test group mirroring!
#
def testGroupMirror(args):
    driver.initThreeHostMultiHomeNoMirrors()
    confProvider = configInterface.getConfigurationProvider().initializeProvider(5432)
    gpArray = confProvider.loadSystemConfig(useUtilityMode=False)

    calc = GpMirrorBuildCalculator(gpArray, 1000, [ "/data/m1", "/data/m2"], [{},{}])
    calc.getGroupMirrors()

    GpMirrorListToBuild([], None, False, 1).checkForPortAndDirectoryConflicts(gpArray)

    testOutput("")
    testOutputGpArray(gpArray)
    testOutput("")
simple_test("testGroupMirror", testGroupMirror, [],
"""
dbid  | content  | role  | preferred_role  | mode  | status  | hostname     | address        | port   | datadir                         | replication_port
1     | -1       | p     | p               | s     | u       | master-host  | primary-host   | 5432   | /datadirpathdbmaster/gp-1       | None
2     | 0        | p     | p               | r     | u       | first-host   | first-host-1   | 50001  | /first/datadirpathdbfast1/gp0   | 53001
3     | 1        | p     | p               | r     | u       | first-host   | first-host-2   | 50002  | /first/datadirpathdbfast2/gp1   | 53002
4     | 2        | p     | p               | r     | u       | second-host  | second-host-1  | 50001  | /second/datadirpathdbfast1/gp2  | 53001
5     | 3        | p     | p               | r     | u       | second-host  | second-host-2  | 50002  | /second/datadirpathdbfast2/gp3  | 53002
6     | 4        | p     | p               | r     | u       | third-host   | third-host-1   | 50001  | /third/datadirpathdbfast2/gp4   | 53001
7     | 5        | p     | p               | r     | u       | third-host   | third-host-2   | 50002  | /third/datadirpathdbfast2/gp5   | 53002
8     | 0        | m     | m               | r     | u       | second-host  | second-host-1  | 51001  | /data/m1                        | 52001
9     | 1        | m     | m               | r     | u       | second-host  | second-host-2  | 51002  | /data/m2                        | 52002
10    | 2        | m     | m               | r     | u       | third-host   | third-host-1   | 51001  | /data/m1                        | 52001
11    | 3        | m     | m               | r     | u       | third-host   | third-host-2   | 51002  | /data/m2                        | 52002
12    | 4        | m     | m               | r     | u       | first-host   | first-host-1   | 51001  | /data/m1                        | 52001
13    | 5        | m     | m               | r     | u       | first-host   | first-host-2   | 51002  | /data/m2                        | 52002
"""

)

#
# Test segment copy and comparison
#
def testCopyAndComparison(args):
    driver.initThreeHostMultiHomeNoMirrors()
    gpArray = configInterface.getConfigurationProvider().initializeProvider(5432).loadSystemConfig(useUtilityMode=False)

    testOutput("")
    for seg in gpArray.getSegDbList():
        seg.getSegmentFilespaces()[12334] = "/data/foo1"
        seg.getSegmentFilespaces()[42334] = "/data/foo2"
        seg.getSegmentFilespaces()[32334] = "/data/foo3"
        seg.getSegmentFilespaces()[72334] = "/data/foo4"

        segCopy = seg.copy()
        testOutput("equalsCopy: %s" % (segCopy == seg))
        if segCopy != seg:
            testOutput("%s" % repr(seg))
            testOutput("%s" % repr(segCopy))
    testOutput("")

simple_test("testCopyAndComparison", testCopyAndComparison, [],
"""
equalsCopy: True
equalsCopy: True
equalsCopy: True
equalsCopy: True
equalsCopy: True
equalsCopy: True
""")

# All done tests
printTestResults()
