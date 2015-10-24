import os

from gppylib import gplog
from gppylib.commands.base import Command, WorkerPool
from gppylib.commands.gp import get_masterdatadir, SendFilerepVerifyMessage
from gppylib.commands.unix import Scp
from gppylib.db import dbconn
from gppylib.db.dbconn import UnexpectedRowsError
from gppylib.gparray import GpArray, FAULT_STRATEGY_LABELS, FAULT_STRATEGY_FILE_REPLICATION
from gppylib.mainUtils import ExceptionNoStackTraceNeeded
from gppylib.operations import Operation
from gppylib.operations.utils import ParallelOperation, RemoteOperation
from gppylib.operations.unix import CheckDir, MakeDir, RemoveTree

# TODO: None of these operations are using master_port to differentiate between various clusters
# Defer until a gpverify feature request comes in.

logger = gplog.get_default_logger()

class VerifyFilerep(Operation):
    def __init__(self, content, token, full, verify_file, verify_dir, ignore_dir, ignore_file, results_level, batch_default):
        self.content = content
        self.token = token
        self.full = full
        self.verify_file = verify_file
        self.verify_dir = verify_dir
        self.ignore_dir = ignore_dir
        self.ignore_file = ignore_file
        self.results_level = results_level
        self.batch_default = batch_default
    def execute(self):
        try:
            ValidateVerificationEntry(token = self.token).run()
            raise ExceptionNoStackTraceNeeded('Token "%s" has already been used. Use "gpverify --clean --token %s" to reclaim this token.' % (self.token, self.token))
        except TokenNotFound:
            pass
        TriggerFilerepVerifyMessages(content = self.content,
                                     token = self.token,
                                     full = self.full,
                                     verify_file = self.verify_file,
                                     verify_dir = self.verify_dir,
                                     ignore_dir = self.ignore_dir,
                                     ignore_file = self.ignore_file,
                                     results_level = self.results_level,
                                     batch_default = self.batch_default).run()
        verify_type = VerificationType.translate(self.full, self.verify_file, self.verify_dir)
        verify_content = self.content if self.content is not None else -1
        InsertVerificationEntry(token = self.token,
                                type = verify_type,
                                content = verify_content).run()
        logger.info('Verification %s has started.' % self.token)



# TODO: If a particular segment's gp_primarymirror fails, then here we're considering that a failure and no
# record gets inserted into gp_verification_history. Subsequent invocations of VerifyFilerep will fail because
# verifications are already running on the other N - 1 segments. 
# A gp_verification_history schema which supports multiple contents would solve this. This would allow per-content
# verifications to be tracked truthfully, as opposed to the current, lossy approach.
class TriggerFilerepVerifyMessages(Operation):
    def __init__(self, content, token, batch_default, full=None, verify_file=None, verify_dir=None,
                       abort=None, suspend=None, resume=None, ignore_dir=None, ignore_file=None,
                       results=None, results_level=None):
        self.content = content
        self.token = token
        self.full = full
        self.verify_file = verify_file
        self.verify_dir = verify_dir
        self.abort = abort
        self.suspend = suspend
        self.resume = resume
        self.ignore_dir = ignore_dir
        self.ignore_file = ignore_file
        self.results = results
        self.results_level = results_level
        self.batch_default = batch_default
        self.pool = None
    def execute(self):
        """
        Sends arbitrary gp_primarymirror requests to the backend processes defined.
        """
        to_trigger = ValidateVerification(content = self.content).run()

        logger.info('Sending gp_primarymirror requests...')
        self.pool = WorkerPool(min(len(to_trigger), self.batch_default))

        for pseg in to_trigger:
            host, port = pseg.getSegmentHostName(), pseg.getSegmentPort()
            cmd = SendFilerepVerifyMessage(name = 'verify %s' % host, host = host, port = port,
                                           token = self.token,
                                           full = self.full,
                                           verify_file = self.verify_file,
                                           verify_dir = self.verify_dir,
                                           abort = self.abort,
                                           suspend = self.suspend,
                                           resume = self.resume,
                                           ignore_dir = self.ignore_dir,
                                           ignore_file = self.ignore_file,
                                           results = self.results,
                                           results_level = self.results_level)
            logger.debug("Sending request to %s:%d" % (host, port))
            self.pool.addCommand(cmd)

        logger.info('Waiting for gp_primarymirror commands to complete...')
        self.pool.wait_and_printdots(len(to_trigger))

        for cmd in self.pool.getCompletedItems():
            res = cmd.get_results()
            if not res.wasSuccessful():
                logger.error('Failed to send gp_primarymirror message to %s:%s' % (cmd.host, cmd.port))
                logger.error('Error: %s' % res.stderr)
                raise TriggerGpPrimaryMirrorFailure()
        logger.info('gp_primarymirror messages have been triggered succesfully.')
class TriggerGpPrimaryMirrorFailure(Exception): pass




class ValidateVerification(Operation):
    def __init__(self, content, primaries_only = True):
        """
        content and primaries_only dictate which portions of the gparray should
        be returned. Their effects are cumulative, meaning:
            content = None  and primaries_only = False  =>  all QEs
            content = None  and primaries_only = True   =>  all primary segments
            content = x     and primaries_only = False  =>  all segments with content x
            content = x     and primaries_only = True   =>  the primary with content x
        """ 
        self.content = content if content >= 0 else None
        self.primaries_only = primaries_only
    def execute(self):
        dburl = dbconn.DbURL()
        gparray = GpArray.initFromCatalog(dburl)
        my_fault_strategy = gparray.getFaultStrategy()
        if my_fault_strategy != FAULT_STRATEGY_FILE_REPLICATION:
            raise NoMirroringError('Fault strategy %s does not support mirror verification.' % FAULT_STRATEGY_LABELS[my_fault_strategy])

        if self.content is not None:
            contents = set( [seg.getSegmentContentId() for seg in gparray.getDbList()] )
            if self.content not in contents:
                raise InvalidContentIDError(self.content)

        logger.info('Validating target contents...')
        to_verify = [x for x in gparray.getDbList() if x.isSegmentQE()]
        if self.content is not None:
            to_verify = [x for x in to_verify if x.getSegmentContentId() == self.content]
        if self.primaries_only:
            to_verify = [x for x in to_verify if x.isSegmentPrimary(current_role=True)]
        return to_verify
class NoMirroringError(Exception): pass
class InvalidContentIDError(Exception):
    def __init__(self, content):
        Exception.__init__(self, "%d is not a valid content ID." % content)


class AbortVerification(Operation):
    def __init__(self, token, batch_default):
        self.token = token
        self.batch_default = batch_default
    def execute(self):
        entry = ValidateVerificationEntry(token = self.token).run()
        if entry['verdone']:
            raise WrongStateError("Only unfinished verification tasks may be aborted.")
        TriggerFilerepVerifyMessages(content = entry['vercontent'],
                                     batch_default = self.batch_default,
                                     token = self.token,
                                     abort = True).run()
        UpdateVerificationEntry(token = self.token, 
                                state = VerificationState.ABORTED,
                                done = True).run()
        logger.info('Verification %s has been aborted.' % self.token)



class SuspendVerification(Operation):
    def __init__(self, token, batch_default):
        self.token = token
        self.batch_default = batch_default
    def execute(self):
        entry = ValidateVerificationEntry(token = self.token).run()
        if entry['verstate'] != VerificationState.RUNNING:
            raise WrongStateError("Only running verification tasks may be resumed.")
        TriggerFilerepVerifyMessages(content = entry['vercontent'],
                                     batch_default = self.batch_default,
                                     token = self.token,
                                     suspend = True).run()
        UpdateVerificationEntry(token = self.token,
                                state = VerificationState.SUSPENDED).run()
        logger.info('Verification %s has been suspended.' % self.token)



class ResumeVerification(Operation):
    def __init__(self, token, batch_default):
        self.token = token
        self.batch_default = batch_default
    def execute(self):
        entry = ValidateVerificationEntry(token = self.token).run()
        if entry['verstate'] != VerificationState.SUSPENDED:
            raise WrongStateError("Only suspended verification tasks may be resumed.")
        TriggerFilerepVerifyMessages(content = entry['vercontent'],
                                     batch_default = self.batch_default,
                                     token = self.token,
                                     resume = True).run()
        UpdateVerificationEntry(token = self.token,
                                state = VerificationState.RUNNING).run()
        logger.info('Verification %s has been resumed.' % self.token)



class CleanDoneVerifications(Operation):
    def __init__(self, batch_default):
        self.batch_default = batch_default
    def execute(self):
        tokens = []
        entries = GetAllVerifications().run()
        entries = [entry for entry in entries if entry['verdone']]
        if len(entries) == 0:
            logger.info("There are no completed verifications to clean up.")
            return
        for entry in entries:
            tokens.append(entry['vertoken'])
            CleanVerification(token = entry['vertoken'],
                              batch_default = self.batch_default).run()   
        logger.info("The following verifications have been cleaned up: %s" % ",".join(tokens))



class CleanVerification(Operation):
    def __init__(self, token, batch_default):
        self.token = token
        self.batch_default = batch_default
        self.pool = None
    def execute(self):
        entry = ValidateVerificationEntry(token = self.token).run()
        if not entry['verdone']:   
            raise WrongStateError("Only finished verification tasks may be cleaned up.")

        path = os.path.join(get_masterdatadir(), 'pg_verify', self.token)
        Command('cleanup', 'rm -rf %s' % path).run(validateAfter=True)
        #RemoveTree(path).run()

        to_clean = ValidateVerification(content = entry['vercontent'],
                                        primaries_only = False).run()
        self.pool = WorkerPool(min(len(to_clean), self.batch_default))
        for seg in to_clean:
            host = seg.getSegmentHostName()
            path = os.path.join(seg.getSegmentDataDirectory(), 'pg_verify', "*%s*" % self.token)
            cmd = Command('cleanup', 'rm -f %s' % path, remoteHost=host)
            self.pool.addCommand(cmd)

        logger.info('Waiting for clean commands to complete...')
        self.pool.wait_and_printdots(len(to_clean))

        for cmd in self.pool.getCompletedItems():
            res = cmd.get_results()
            if not res.wasSuccessful():
                logger.error('Failed to send cleanup on %s' % cmd.host)
                logger.error('Error: %s' % res.stderr)
                raise CleanVerificationError()
        RemoveVerificationEntry(token = self.token).run()
        logger.info('Verification %s has been cleaned.' % self.token)
class CleanVerificationError(Exception): pass
        
        

class FinalizeAllVerifications(Operation):
    def __init__(self, batch_default):
        self.batch_default = batch_default
    def execute(self):
        ret = []
        entries = GetAllVerifications().run()
        for entry in entries:
            updated_entry = FinalizeVerification(token = entry['vertoken'],
                                                 batch_default = self.batch_default).run()
            ret.append(updated_entry)
        return ret



class FinalizeVerification(Operation):
    def __init__(self, token, batch_default):
        self.token = token
        self.batch_default = batch_default
    def execute(self):
        entry = ValidateVerificationEntry(token = self.token).run()
        to_inspect = ValidateVerification(content = entry['vercontent']).run()

        state_dict = ValidateCompletion(token = self.token, 
                                        to_validate = to_inspect,
                                        batch_default = self.batch_default).run()
        incomplete = state_dict[VerificationState.RUNNING]
        if len(incomplete) > 0:
            # TODO: --force to consolidate files despite ongoing
            logger.error('One or more content verifications is still in progress: %s' % incomplete)
            return entry

        GatherResults(master_datadir = get_masterdatadir(),
                      content = entry['vercontent'],
                      token = self.token,
                      batch_default = self.batch_default).run()

        state = VerificationState.SUCCEEDED
        mismatch = False
        aborted = state_dict[VerificationState.ABORTED]
        failed = state_dict[VerificationState.FAILED]
        if len(failed) > 0:                             # any FAILED trumps ABORTED
            state = VerificationState.FAILED
            mismatch = True
            logger.warn('One or more contents for verification %s were marked FAILED: %s' % (self.token, failed))
        elif len(aborted) > 0:
            state = VerificationState.ABORTED
            logger.warn('One or more contents for verification %s were marked ABORTED: %s' % (self.token, aborted))
        else:
            logger.info('Verification %s completed successfully' % self.token)
        if not entry['verdone']:
            UpdateVerificationEntry(token = self.token,
                                    state = state,
                                    mismatch = mismatch,
                                    done = True).run()
            entry.update({ 'vermismatch' : mismatch,
                           'verdone' : True,
                           'verstate' : state })
        return entry



class GatherResults(Operation):
    def __init__(self, master_datadir, token, content, batch_default):
        self.master_datadir = master_datadir
        self.token = token
        self.content = content
        self.batch_default = batch_default
        self.pool = None
    def execute(self):
        logger.info('Gathering results of verification %s...' % self.token)
        to_gather = ValidateVerification(content = self.content,
                                         primaries_only = False).run()

        dest_base = os.path.join(self.master_datadir, 'pg_verify', self.token)
        if CheckDir(dest_base).run():
            # TODO: if end user has mucked around with artifacts on master, a regathering may
            # be needed; perhaps, a --force option to accompany --results?
            return
        MakeDir(dest_base).run()

        self.pool = WorkerPool(min(len(to_gather), self.batch_default))
        for seg in to_gather:
            host = seg.getSegmentHostName()
            content = seg.getSegmentContentId()
            role = seg.getSegmentRole()
            src = os.path.join(seg.getSegmentDataDirectory(), "pg_verify", "*%s*" % self.token)

            dest = os.path.join(dest_base, str(content), str(role))
            MakeDir(dest).run()
            cmd = Scp('consolidate', srcFile=src, srcHost=host, dstFile=dest)
            self.pool.addCommand(cmd)

        logger.info('Waiting for scp commands to complete...')
        self.pool.wait_and_printdots(len(to_gather))
        self.pool.check_results()

        dest = os.path.join(dest_base, 'verification_%s.fix' % self.token)
        with open(dest, 'w') as output:
            for seg in to_gather:
                content = seg.getSegmentContentId()
                role = seg.getSegmentRole()
                src = os.path.join(dest_base, str(content), str(role), 'verification_%s.fix' % self.token)
                with open(src, 'r') as input:
                    output.writelines(input.readlines())
        


class ValidateCompletion(Operation):
    def __init__(self, token, to_validate, batch_default):
        self.token = token
        self.to_validate = to_validate
        self.batch_default = batch_default
    def execute(self):
        state_dict = { VerificationState.RUNNING: [],
                       VerificationState.SUCCEEDED: [],
                       VerificationState.ABORTED: [],
                       VerificationState.FAILED: [] }
        operations = []
        for pseg in self.to_validate:
            operations.append(RemoteOperation(ValidateResultFile(token = self.token,
                                                                 datadir = pseg.getSegmentDataDirectory(),
                                                                 content = pseg.getSegmentContentId()),
                                              pseg.getSegmentHostName()))
        ParallelOperation(operations, self.batch_default).run()
        for remote in operations:
            state = remote.get_ret()
            state_dict[state].append(remote.operation.content)
        return state_dict



class ValidateResultFile(Operation):
    def __init__(self, token, datadir, content):
        self.token = token
        self.datadir = datadir
        self.content = content
        self.translate = { 'SUCCESS': VerificationState.SUCCEEDED,
                           'FAILED': VerificationState.FAILED,
                           'ABORT': VerificationState.ABORTED }
    def execute(self):
        path = os.path.join(self.datadir, "pg_verify", "verification_%s.result" % self.token)
        with open(path, 'r') as f:
            phrase = f.readline().strip()
        return self.translate.get(phrase, VerificationState.RUNNING)



class VerificationType:
    FULL, FILE, DIR = range(3)
    lookup = ['FULL', 'FILE', 'DIR']
    @staticmethod
    def translate(full, file, dir):
        type_tuple = (full, file, dir)                          # tuple is ordered to emulate the enum
        type_flags = [int(bool(type)) for type in type_tuple]   # list of 1's and 0's where 1's denote truth 
        if sum(type_flags) != 1:
            raise InvalidVerificationType()
        return type_flags.index(1)                              # index of 1 in type_tuple will correspond to desired enum value
class InvalidVerificationType(Exception): pass

# TODO: This is disgusting. Python needs enum! with __str__! See pypi Enum package.
class VerificationState:
    RUNNING, SUSPENDED, ABORTED, FAILED, SUCCEEDED = range(5)
    lookup = [ 'RUNNING', 'SUSPENDED', 'ABORTED', 'FAILED', 'SUCCEEDED' ]
class WrongStateError(Exception): pass


class ValidateVerificationEntry(Operation):
    SELECT_VERIFICATION_ENTRY = """
        select vertoken, vertype, vercontent, verstarttime, verstate, verdone, verendtime, vermismatch 
        from gp_verification_history where vertoken = '%s';
    """
    def __init__(self, token):
        self.token = token
    def execute(self):
        dburl = dbconn.DbURL()
        query = self.SELECT_VERIFICATION_ENTRY % self.token
        with dbconn.connect(dburl) as conn:
            try:
                tuple = dbconn.execSQLForSingletonRow(conn, query)
            except UnexpectedRowsError, e:
                if e.actual == 0:
                    raise TokenNotFound(self.token)
                raise
        # TODO: execSQL or pygresql should be able to do this for us
        ret = { 'vertoken': tuple[0],
                'vertype': tuple[1],
                'vercontent': tuple[2],
                'verstarttime': tuple[3],
                'verstate': tuple[4],
                'verdone': tuple[5],
                'verendtime': tuple[6],
                'vermismatch': tuple[7] }
        logger.debug("ValidateVerificationEntry: %s" % ret)
        return ret
class TokenNotFound(Exception):
    def __init__(self, token):
        Exception.__init__(self, "Token %s was not found." % token)



class InsertVerificationEntry(Operation):
    INSERT_VERIFICATION_ENTRY = """
        insert into gp_verification_history 
        (vertoken, vertype, vercontent, verstarttime, verstate, verdone, verendtime, vermismatch)
        values ('%s', %d, %d, now(), %d, false, now(), false);
    """
    def __init__(self, token, type, content):
        self.token = token
        self.type = type
        self.content = content
    def execute(self):
        dburl = dbconn.DbURL()
        query = self.INSERT_VERIFICATION_ENTRY % (self.token, self.type, self.content, VerificationState.RUNNING)
        with dbconn.connect(dburl, allowSystemTableMods='dml') as conn:
            dbconn.execSQL(conn, query)
            conn.commit()



class UpdateVerificationEntry(Operation):
    UPDATE_VERIFICATION_ENTRY = "update gp_verification_history set verstate = %d, verdone = %s, vermismatch = %s, verendtime = now() where vertoken = '%s';"
    def __init__(self, token, state, done=False, mismatch=False):
        self.token = token
        self.state = state
        self.done = done
        self.mismatch = mismatch
    def execute(self):
        dburl = dbconn.DbURL()
        query = self.UPDATE_VERIFICATION_ENTRY % (self.state, self.done, self.mismatch, self.token)
        with dbconn.connect(dburl, allowSystemTableMods='dml') as conn:
            dbconn.execSQL(conn, query)
            conn.commit()



class RemoveVerificationEntry(Operation):
    REMOVE_VERIFICATION_ENTRY = "delete from gp_verification_history where vertoken = '%s';"
    def __init__(self, token):
        self.token = token
    def execute(self):
        dburl = dbconn.DbURL()
        query = self.REMOVE_VERIFICATION_ENTRY % self.token
        with dbconn.connect(dburl, allowSystemTableMods='dml') as conn:
            dbconn.execSQL(conn, query)
            conn.commit()



class GetAllVerifications(Operation):
    SELECT_ALL_VERIFICATIONS = """
        select vertoken, vertype, vercontent, verstarttime, verstate, verdone, verendtime, vermismatch 
        from gp_verification_history order by verstarttime ASC;
    """
    def __init__(self): pass
    def execute(self):
        ret = []
        dburl = dbconn.DbURL()
        with dbconn.connect(dburl) as conn:
            # TODO: improve execSQL APIs to avoid need to use cursor here for such a simple task
            cursor=conn.cursor()
            cursor.execute(self.SELECT_ALL_VERIFICATIONS)
            res = cursor.fetchall()
            cursor.close()
        for tuple in res:
            # TODO: execSQL or pygresql should be able to do this for us
            ret.append({ 'vertoken': tuple[0],
                         'vertype': tuple[1],
                         'vercontent': tuple[2],
                         'verstarttime': tuple[3],
                         'verstate': tuple[4],
                         'verdone': tuple[5],
                         'verendtime': tuple[6],
                         'vermismatch': tuple[7] })
        return ret
        


