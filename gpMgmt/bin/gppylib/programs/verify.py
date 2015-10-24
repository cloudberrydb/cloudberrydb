'''
Created on May 9, 2011

'''

from gppylib.mainUtils import addStandardLoggingAndHelpOptions, ProgramArgumentValidationException

import os
from datetime import datetime
from optparse import OptionGroup

from gppylib import gplog
from gppylib import userinput
from gppylib.commands.gp import get_masterdatadir
from gppylib.gpparseopts import OptParser, OptChecker
from gppylib.mainUtils import UserAbortedException
from gppylib.operations import Operation
from gppylib.operations.verify import AbortVerification, CleanVerification, CleanDoneVerifications, FinalizeVerification, FinalizeAllVerifications, ResumeVerification, SuspendVerification, VerificationType, VerifyFilerep, VerificationState

logger = gplog.get_default_logger()

MAX_BATCH_SIZE=128

class GpVerifyProgram(Operation):
    def __init__(self, options, args):
        count = sum([1 for opt in ['full', 'verify_file', 'verify_dir', 'clean', 'results', 'abort', 'suspend', 'resume']
                       if options.__dict__[opt] is not None])
        if count == 0:
            raise ProgramArgumentValidationException("""Must specify one verify request type (--full, --file, or --directorytree)
                                                        or action (--clean, --results, --abort, --suspend, or --resume)""")
        elif count > 1:
            raise ProgramArgumentValidationException("""Only specify one verify request type (--full, --file, or --directorytree)
                                                        or action (--clean, --results, --abort, --suspend, or --resume)""")
        self.full = options.full
        self.verify_file = options.verify_file
        self.verify_dir = options.verify_dir
        self.results = options.results
        self.clean = options.clean
        self.abort = options.abort
        self.suspend = options.suspend
        self.resume = options.resume

        count = sum([1 for opt in ['full', 'verify_file', 'verify_dir']
                       if options.__dict__[opt] is not None])
        if options.ignore_file is not None and count != 1:
            raise ProgramArgumentValidationException("--fileignore must be used with --full, --file, or --directorytree")
        if options.ignore_dir is not None and count != 1:
            raise ProgramArgumentValidationException("--dirignore must be used with --full, --file, or --directorytree") 
        if options.results_level is not None and count != 1:
            raise ProgramArgumentValidationException("--resultslevel must be used with --full, --file, or --directorytree")
        self.ignore_file = options.ignore_file
        self.ignore_dir = options.ignore_dir
        self.results_level = options.results_level

        if options.parallel < 1 or options.parallel > MAX_BATCH_SIZE:
            raise ProgramArgumentValidationException('Parallelism value must be between 1 and %d' % MAX_BATCH_SIZE)
        self.batch_default = options.parallel

        self.content = None
        if options.content is not None:
            if self.clean or self.results or self.abort or self.suspend or self.resume:
                raise ProgramArgumentValidationException("--content can only be used when spooling new verifications, with --full, --file, or --directorytree.")
            self.content = int(options.content)

        if options.token is None:
            if self.abort or self.suspend or self.resume:
                raise ProgramArgumentValidationException("If --abort, --suspend, or --resume is provided, --token must be given.")
        self.token = options.token

    def execute(self):
        """
        Based on options, sends different verification messages to the backend.
        """
        if self.results:
            self._do_results(self.token, self.batch_default)
        elif self.clean:
            self._do_clean(self.token, self.batch_default)
        elif self.abort:
            AbortVerification(token = self.token,
                              batch_default = self.batch_default).run()
        elif self.resume:
            ResumeVerification(token = self.token,
                               batch_default = self.batch_default).run()
        elif self.suspend:
            SuspendVerification(token = self.token,
                                batch_default = self.batch_default).run()
        else:
            if self.token is None:
                self.token = datetime.now().strftime("%Y%m%d%H%M%S")
            VerifyFilerep(content = self.content,
                          full = self.full,
                          verify_file = self.verify_file,
                          verify_dir = self.verify_dir,
                          token = self.token,
                          ignore_dir = self.ignore_dir,
                          ignore_file = self.ignore_file,
                          results_level = self.results_level,
                          batch_default = self.batch_default).run()

    def _do_results(self, token, batch_default):
        if self.token is not None:
            entry = FinalizeVerification(token = token,
                                         batch_default = batch_default).run()
            entries = [entry]
        else:
            entries = FinalizeAllVerifications(batch_default).run()
        master_datadir = get_masterdatadir()
        for entry in entries: 
            logger.info('---------------------------')
            logger.info('Token:    %s' % entry['vertoken'])
            logger.info('Type:     %s' % VerificationType.lookup[entry['vertype']])
            logger.info('Content:  %s' % (entry['vercontent'] if entry['vercontent'] > 0 else "ALL"))
            logger.info('Started:  %s' % entry['verstarttime'])
            logger.info('State:    %s' % VerificationState.lookup[entry['verstate']])
            if entry['verdone']:
                path = os.path.join(master_datadir, 'pg_verify', entry['vertoken'])
                logger.info('Details:  %s' % path)

    def _do_clean(self, token, batch_default):
        if self.token is not None:
            CleanVerification(token = self.token,
                              batch_default = self.batch_default).run()
        else:
            if not userinput.ask_yesno(None, "\nAre you sure you want to remove all completed verification artifacts across the cluster?", 'N'):
                raise UserAbortedException()
            CleanDoneVerifications(batch_default = self.batch_default).run()
    
    def cleanup(self): pass
    
    @staticmethod
    def createParser():
        """
        Creates the command line options parser object for gpverify.
        """
        
        description = ("Initiates primary/mirror verification.")
        help = []

        parser = OptParser(option_class=OptChecker,
                    description=' '.join(description.split()),
                    version='%prog version $Revision: #1 $')
        parser.setHelp(help)

        addStandardLoggingAndHelpOptions(parser, True)
        
        addTo = OptionGroup(parser, "Request Type")
        parser.add_option_group(addTo)
        addTo.add_option('--full', dest='full', action='store_true',
                         help='Perform a full verification pass.  Use --token option to ' \
                         'give the verification pass an identifier.')
        addTo.add_option('--file', dest='verify_file', metavar='<file>',
                         help='Based on file type, perform either a physical or logical verification of <file>.  ' \
                         'Use --token option to give the verification request an identifier.')
        addTo.add_option('--directorytree', dest='verify_dir',
                         metavar='<verify_dir>',
                         help='Perform a full verification pass on the specified directory.  ' \
                         'Use --token option to assign the verification pass an identifier.' )

        addTo = OptionGroup(parser, "Request Options")
        parser.add_option_group(addTo)
        addTo.add_option('--token', dest='token', metavar='<token>',
                         help='A token to use for the request.  ' \
                         'This identifier will be used in the logs and can be used to identify ' \
                         'a verification pass to the --abort, --suspend, --resume and --results ' \
                         'options.')

        addTo.add_option('-c', '--content', dest='content',
                         metavar='<content_id>', 
                         help='Send verification request only to the primary segment with the given <content_id>.')
        addTo.add_option('--abort', dest='abort', action='store_true',
                         help='Abort a verification request that is in progress.  ' \
                         'Can use --token option to abort a specific verification request.')
        addTo.add_option('--suspend', dest='suspend', action='store_true',
                         help='Suspend a verification request that is in progress.' \
                         'Can use --token option to suspend a specific verification request.')
        addTo.add_option('--resume', dest='resume', action='store_true',
                         help='Resume a suspended verification request.  Can use the ' \
                         '--token option to resume a specific verification request.')
        addTo.add_option('--fileignore', dest='ignore_file', metavar='<ignore_file>', 
                         help='Ignore any filenames matching <ignore_file>.  Multiple ' \
                         'files can be specified using a comma separated list.')
        addTo.add_option('--dirignore', dest='ignore_dir', metavar='<ignore_dir>', 
                         help='Ignore any directories matching <ignore_dir>.  Multiple ' \
                         'directories can be specified using a comma separated list.')

        addTo = OptionGroup(parser, "Reporting Options")
        parser.add_option_group(addTo)
        addTo.add_option('--results', dest='results', action='store_true',
                         help='Display verification results.  Can use' \
                         'the --token option to view results of a specific verification request.')
        addTo.add_option('--resultslevel', dest='results_level', action='store',
                         metavar='<detail_level>', type=int,
                         help='Level of detail to show for results. Valid levels are from 1 to 10.')
        addTo.add_option('--clean', dest='clean', action='store_true',
                         help='Clean up verification artifacts and the gp_verification_history table.')
        
        addTo = OptionGroup(parser, "Misc. Options")
        parser.add_option_group(addTo)
        addTo.add_option('-B', '--parallel', action='store', default=64,
                         type=int, help='Number of worker threads used to send verification requests.')
        
       
        parser.set_defaults()
        return parser
