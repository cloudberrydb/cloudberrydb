from gppylib.commands.base import Command, ExecutionError, REMOTE, WorkerPool
from gppylib.db import dbconn
from gppylib.gparray import GpArray
from gppylib.test.behave_utils.utils import run_gpcommand, getRows
import platform

@given('the information of a "{seg}" segment on a remote host is saved')
@when('the information of a "{seg}" segment on a remote host is saved')
@then('the information of a "{seg}" segment on a remote host is saved')
def impl(context, seg):
    if seg == "mirror":
        gparray = GpArray.initFromCatalog(dbconn.DbURL())
        mirror_segs = [seg for seg in gparray.getDbList() if seg.isSegmentMirror() and seg.getSegmentHostName() != platform.node()]
        context.remote_mirror_segdbId = mirror_segs[0].getSegmentDbId()
        context.remote_mirror_segdbname = mirror_segs[0].getSegmentHostName()
        context.remote_mirror_datadir = mirror_segs[0].getSegmentDataDirectory()

@given('user runs the command "{cmd}" with the saved mirror segment option')
@when('user runs the command "{cmd}" with the saved mirror segment option')
@then('user runs the command "{cmd}" with the saved mirror segment option')
def impl(context, cmd):
    cmdStr = '%s -s %s' % (cmd, int(context.remote_mirror_segdbId))
    cmd=Command(name='user command', cmdStr=cmdStr)
    cmd.run(validateAfter=True)

@given('the saved mirror segment process is still running on that host')
@when('the saved mirror segment process is still running on that host')
@then('the saved mirror segment process is still running on that host')
def impl(context):
    cmd = """ps ux | grep "/bin/postgres \-D %s " | grep -v grep""" % (context.remote_mirror_datadir)
    cmd=Command(name='user command', cmdStr=cmd, ctxt=REMOTE, remoteHost=context.remote_mirror_segdbname)
    cmd.run(validateAfter=True)
    res = cmd.get_results()
    if not res.stdout.strip():
        raise Exception('Mirror segment "%s" not active on "%s"' % (context.remote_mirror_datadir, context.remote_mirror_segdbname))
    
@given('the saved mirror segment is marked down in config')
@when('the saved mirror segment is marked down in config')
@then('the saved mirror segment is marked down in config')
def impl(context):
    qry = """select count(*) from gp_segment_configuration where status='d' and hostname='%s' and dbid=%s""" % (context.remote_mirror_segdbname, context.remote_mirror_segdbId)
    row_count = getRows('template1', qry)[0][0]
    if row_count != 1:
        raise Exception('Expected mirror segment %s on host %s to be down, but it is running.' % (context.remote_mirror_datadir, context.remote_mirror_segdbname))
