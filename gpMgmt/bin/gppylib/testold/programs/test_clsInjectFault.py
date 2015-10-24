#!/usr/bin/env python
#
#
from gppylib.testold.testDriver import TestDriver
from gppylib.programs.clsInjectFault import GpInjectFaultProgram
from gppylib.mainUtils import *
from gppylib.testold.testUtils import *
from gppylib.system import fileSystemImplTest, fileSystemInterface
from gppylib.gplog import get_default_logger

programName = sys.argv[0]
parserFn = GpInjectFaultProgram.createParser
commandFn = GpInjectFaultProgram.createProgram

driver = TestDriver()
logger = get_default_logger()

class TempFileLogger:
    def __init__(self, index, driver):
        self.__index = index
        self.__driver = driver

    def generate(self):
        index = self.__index
        fs = self.__driver.getFileSystem()
        if fs.hasTemporaryFileDataForTesting(index):
            testOutput("BEGIN File %s:" % index)
            testOutput(fs.getTemporaryFileDataForTesting(index))
            testOutput(":END File %s" % index)
        else:
            testOutput("FILE %s NOT GENERATED" % index)

class CommandSimulatorCommand:
    def __init__(self, stdout, stderr, errorCode, numRepeats):
        self.__stdout = stdout
        self.__stderr = stderr
        self.__errorCode = errorCode
        self.__numRepeats = numRepeats

    def getStdout(self): return self.__stdout
    def getStderr(self): return self.__stderr
    def getErrorCode(self): return self.__errorCode
    def getNumRepeats(self): return self.__numRepeats

class CommandSimulatorForTest:
    def __init__(self, commandSimulatorCommands ):
        self.__commandSimulatorCommands = commandSimulatorCommands
        self.__index = 0
        self.__countAtIndex = 0

    #
    # returns (errorCode, stdoutArr, stderrArr)
    #
    def simulate(self, cmdStr):
        while self.__countAtIndex >= self.__commandSimulatorCommands[self.__index].getNumRepeats():
            self.__countAtIndex = 0
            self.__index += 1

        cmd = self.__commandSimulatorCommands[self.__index]
        # logger.info("Simulation got %s " % " ".join(cmd.getStderr()))
        self.__countAtIndex += 1
        return (cmd.getErrorCode(), cmd.getStdout(), cmd.getStderr())

###############
#
# Now the tests:
#
# First test: no arguments.
driver.initOneHostConfiguration()
test_main( "noArguments", [], parserFn, commandFn, None, \
    """Validation error: neither --host nor --seg_dbid specified.  Exactly one should be specified.""")

driver.initOneHostConfiguration()
test_main( "noRoleArgForHost", ["--host", "this-is-my-host"], parserFn, commandFn, None, \
    """Validation error: --role not specified when --host is specified.  Role is required when targeting a host.""")

driver.initOneHostConfiguration()
test_main( "noHostArgForRole", ["--role", "primary"], parserFn, commandFn, None, \
    """Validation error: neither --host nor --seg_dbid specified.  Exactly one should be specified.""" )

driver.initOneHostConfiguration()
test_main( "noMatchingHost", ["--host", "unknown", "--role", "primary"], parserFn, commandFn, None, \
"""Injecting fault on 0 segment(s)
sys.exit 0"""
)

driver.initOneHostConfiguration()
test_main( "invalidRole", ["--host", "unknown", "--role", "foo"], parserFn, commandFn, None, \
"""Injecting fault on 0 segment(s)
sys.exit 0"""
)

# Contact primaries
driver.initOneHostConfiguration()
test_main( "faultPrimaryNoArgs", ["--host", "this-is-my-host", "--role", "primary"], parserFn, commandFn, \
          [TempFileLogger(0,driver)], \
"""Injecting fault on 2 segment(s)
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 50001 -i /tmp/temporaryNamedFile0
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 50002 -i /tmp/temporaryNamedFile0
sys.exit 0
BEGIN File 0:
faultInject





1
10
:END File 0"""
    )

# Contact mirrors with args
driver.initOneHostConfiguration()
test_main( "faultPrimarySomeArgs", ["--host", "this-is-my-host", "--role", "mirror", \
            "-f", "myFaultName", \
            "-y", "myFaultType", \
            "-c", "create table", \
            "-d", "db1", \
            "-t", "table10", \
            "-o", "55", \
            "-z", "11" \
            ], parserFn, commandFn, \
          [TempFileLogger(0,driver)], \
"""Injecting fault on 2 segment(s)
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60001 -i /tmp/temporaryNamedFile0
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60002 -i /tmp/temporaryNamedFile0
sys.exit 0
BEGIN File 0:
faultInject
myFaultName
myFaultType
create table
db1
table10
55
11
:END File 0"""
    )

#
# sync argument!
#
driver.initOneHostConfiguration()
test_main( "invalidRole", ["--host", "unknown", "--role", "primary", "-m", "fun"], parserFn, commandFn, None, \
"""Invalid -m, --mode option fun""" )

# Contact mirrors with args, sync
driver.initOneHostConfiguration()

# the first one will finish on the 15th query
addCommandSimulator("$GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60001 -i /tmp/temporaryNamedFile1",
        CommandSimulatorForTest([ CommandSimulatorCommand([], ["Success: waitMore"], 0, 14),
                                  CommandSimulatorCommand([], ["Success: done"], 0, 1)]))

# the second one will finish on the second query
addCommandSimulator("$GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60002 -i /tmp/temporaryNamedFile1",
                    CommandSimulatorForTest([ CommandSimulatorCommand([], ["Success: waitMore"], 0, 1),
                                              CommandSimulatorCommand([], ["Success: done"], 0, 1)]))

test_main( "syncTest", ["--mode", "sync", "--host", "this-is-my-host", "--role", "mirror", \
            "-f", "myFaultName", \
            "-y", "myFaultType", \
            "-c", "create table", \
            "-d", "db1", \
            "-t", "table10", \
            "-o", "55", \
            "-z", "11" \
            ], parserFn, commandFn, \
          [TempFileLogger(1,driver)], \
"""Injecting fault on 2 segment(s)
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60001 -i /tmp/temporaryNamedFile0
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60002 -i /tmp/temporaryNamedFile0
Sleeping (seconds): 0.12
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60001 -i /tmp/temporaryNamedFile1
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60002 -i /tmp/temporaryNamedFile1
Sleeping (seconds): 0.17
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60001 -i /tmp/temporaryNamedFile1
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60002 -i /tmp/temporaryNamedFile1
Sleeping (seconds): 0.26
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60001 -i /tmp/temporaryNamedFile1
Sleeping (seconds): 0.39
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60001 -i /tmp/temporaryNamedFile1
Sleeping (seconds): 0.59
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60001 -i /tmp/temporaryNamedFile1
Sleeping (seconds): 0.88
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60001 -i /tmp/temporaryNamedFile1
Sleeping (seconds): 1.32
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60001 -i /tmp/temporaryNamedFile1
Sleeping (seconds): 1.98
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60001 -i /tmp/temporaryNamedFile1
Sleeping (seconds): 2.96
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60001 -i /tmp/temporaryNamedFile1
Sleeping (seconds): 4.44
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60001 -i /tmp/temporaryNamedFile1
Sleeping (seconds): 6.67
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60001 -i /tmp/temporaryNamedFile1
Sleeping (seconds): 10.00
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60001 -i /tmp/temporaryNamedFile1
Sleeping (seconds): 10.00
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60001 -i /tmp/temporaryNamedFile1
Sleeping (seconds): 10.00
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60001 -i /tmp/temporaryNamedFile1
Sleeping (seconds): 10.00
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60001 -i /tmp/temporaryNamedFile1
sys.exit 0
BEGIN File 1:
getFaultInjectStatus
myFaultName
:END File 1""")

clearCommandSimulators()

# test: don't do sync, even if mode is passed, when fault type is reset or status
driver.initOneHostConfiguration()
test_main( "syncNoWaitTest1", ["--mode", "sync", "--host", "this-is-my-host", "--role", "mirror", \
            "-f", "resetFaultName", "-y", "reset", "-c", "create table", "-d", "db1", \
            "-t", "table10", "-o", "55", "-z", "11" \
            ], parserFn, commandFn, None, \
"""Injecting fault on 2 segment(s)
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60001 -i /tmp/temporaryNamedFile0
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60002 -i /tmp/temporaryNamedFile0
sys.exit 0"""
)
driver.initOneHostConfiguration()
test_main( "syncNoWaitTest2", ["--mode", "sync", "--host", "this-is-my-host", "--role", "mirror", \
            "-f", "resetFaultName", "-y", "status", "-c", "create table", "-d", "db1", \
            "-t", "table10", "-o", "55", "-z", "11" \
            ], parserFn, commandFn, None, \
"""Injecting fault on 2 segment(s)
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60001 -i /tmp/temporaryNamedFile0
exec $GPHOME/bin/gp_primarymirror -h this-is-my-host -p 60002 -i /tmp/temporaryNamedFile0
sys.exit 0"""
)


# TODO: test alternate argument forms (--xxx instead of -y)
# TODO: test contacting single specified dbid

# All done tests
printTestResults()
