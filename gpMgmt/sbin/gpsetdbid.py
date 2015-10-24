#!/usr/bin/env python
'''
Copyright (c) Greenplum Inc 2010. All Rights Reserved. 

This is a private script called by the Greenplum Management scripts.

With the script you can create the gp_dbid file within a segment's data
directory.

This script does NOT modify the configuration information stored within
the database.
'''

import os

from optparse            import OptionGroup
from gppylib.gp_dbid     import writeGpDbidFile
from gppylib.mainUtils   import *
from gppylib.gpparseopts import OptParser, OptChecker
from gppylib.gplog       import get_logger_if_verbose

__help__ = [""]

#------------------------------- GpSetDBId --------------------------------
class GpSetDBId:
    """
    Setup a gp_dbid file for a specified directory.
    """
    
    def __init__(self, options):
        self.__directory = options.directory
        self.__dbid      = options.dbid

    def run(self):
        writeGpDbidFile(self.__directory, self.__dbid, logger=get_logger_if_verbose())

    def cleanup(self):
        pass

    #-------------------------------------------------------------------------
    @staticmethod
    def createParser():
        """
        Constructs and returns an option parser.

        Called by simple_main()
        """
        parser = OptParser(option_class=OptChecker,
                           version='%prog version $Revision: $')
        parser.setHelp(__help__)

        addStandardLoggingAndHelpOptions(parser, False)

        opts = OptionGroup(parser, "Required Options")
        opts.add_option('-d', '--directory', type='string')
        opts.add_option('-i', '--dbid', type='int')
        parser.add_option_group(opts)

        parser.set_defaults()
        return parser


    #-------------------------------------------------------------------------
    @staticmethod
    def createProgram(options, args):
        """
        Construct and returns a GpSetDBId object.

        Called by simple_main()
        """
        
        # sanity check
        if len(args) > 0 :
            raise ProgramArgumentValidationException(
                "too many arguments: only options may be specified")
        if not options.directory:
            raise ProgramArgumentValidationException("--directory is required")
        if not options.dbid:
            raise ProgramArgumentValidationException("--dbid is required")
        return GpSetDBId(options)
        

#------------------------------- Mainline --------------------------------
if __name__ == '__main__':
    mainOptions = {
        'suppressStartupLogMessage': True,
        'useHelperToolLogging': True
        }
    simple_main(GpSetDBId.createParser,
                GpSetDBId.createProgram,
                mainOptions)
