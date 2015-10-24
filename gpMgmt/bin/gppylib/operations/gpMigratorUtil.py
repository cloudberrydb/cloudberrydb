#!/usr/bin/env python 
#
# This is a common util lib for gpmigrator and gpmigrator_mirror.
#
# Copyright (c) Greenplum Inc 2010. All Rights Reserved.
#
import pickle, base64
import subprocess, csv
import optparse, tempfile, re, traceback
from datetime import date
from time import localtime, strftime, sleep, time
from threading import Thread
from Queue     import Queue
import urllib    # for passing strings across gpssh
import shutil    # for copying
from pygresql import pg              # Database interaction
from gppylib.gplog import *          # Greenplum logging facility
from gppylib.commands import base    # Greenplum layer for worker pools
from gppylib.commands import unix    # Greenplum layer for unix interaction
from gppylib.commands.gp import GpCreateDBIdFile
from gppylib.db import dbconn
from gppylib.gpversion import GpVersion
from gppylib.gparray import GpArray, GpDB
from gppylib.gphostcache import GpHostCache
from gppylib.gpcoverage import GpCoverage
from gppylib.operations.gpMigratorUtil import *

libdir = os.path.join(sys.path[0], 'lib/')
logger        = get_default_logger()
MIGRATIONUSER = 'gpmigrator'
LOCKEXT       = '.gpmigrator_orig'
WORKDIR       = 'gpmigrator'
BACKUPDIR     = 'backup'
UPGRADEDIR    = 'upgrade'
PARALLELISM   = 16

#============================================================
def make_conn(ug, user, db, options, port, sockdir):
    retries = 5
    for i in range(retries):
        try:
            logger.debug("making database connection: user = %s, "
                         "dbname = %s port = %i" 
                         % (user, db, port))
            conn = pg.connect(user=user,
                              dbname=db, 
                              opt=options,
                              port=port)
            break
        except pg.InternalError, e:
            if 'too many clients already' in str(e) and i < retries:
                logger.warning('Max Connection reached, attempt %d / %d' % (i+1, retries))
                sleep(2)
                continue
            raise ConnectionError(str(e))
    return conn

#============================================================
def cli_help(execname):
    help_path = os.path.join(sys.path[0], '..', 'docs', 'cli_help', execname + '_help');
    f = None
    try:
        try:
            f = open(help_path);
            return f.read(-1)
        except:
            return ''
    finally:
        if f: f.close()

#============================================================
def usage(execname):
    print cli_help(execname) or __doc__

#============================================================
class ConnectionError(StandardError): pass
class UpgradeError(StandardError): pass
class CmdError(StandardError):
    def __init__(self, cmd, stdout, stderr):
        self.cmd = cmd
        self.stdout = stdout
        self.stderr = stderr
    def __str__(self):
        return self.stderr

#============================================================
def is_supported_version(version, upgrade=True):
    '''
    Checks that a given GpVersion Object is supported for upgrade/downgrade.
    We do not support:
      Versions < PREVIOUS MINOR RELEASE
      Versions > main
    '''
    
    if upgrade:
        upstr = "upgrade"
    else:
        upstr = "downgrade"

    build = version.getVersionBuild()
    if not re.match(r"(.*)", build):
        raise UpgradeError(
            "Greenplum Version '%s' is not supported for %s"
            % (str(version), upstr))            

    main = GpVersion('main')
    if version > main:
        raise UpgradeError(
            "To %s Greenplum Version '%s' use the %s tool "
            "shipped with that release" 
            % (upstr, str(version), upstr))
    if version < (main << 1):
        raise UpgradeError(
            "Greenplum Version '%s' is not supported for %s"
            % (str(version), upstr))
    return True
        

#============================================================
class GpUpgradeCmd(base.Command):
    def __init__(self, name, cmd, ctxt=base.LOCAL, remoteHost=None):
        cmdStr = ' '.join(cmd)
        base.Command.__init__(self, name, cmdStr, ctxt, remoteHost)

        
#============================================================
class GPUpgradeBase(object):
    def __init__(self):
        self.cmd        = 'MASTER'      # Command being run
        self.array     = None
        self.dbs        = {}       # dict of databases in the array
        self.dbup       = None
        self.debug      = False
        self.faultinjection = None
        self.hostcache  = None
        self.interrupted = False    # SIGINT recieved
        self.logdir    = None
        self.logfile   = None
        self.masterdir = None     # Master Data Directory
        self.masterport = 5432    # Master Port
        self.mirrors    = []       # list of mirrors
        self.newenv    = None     # enviornment: new gp env
        self.newhome   = None     # New gphome/bin directory
        self.oldenv    = None     # enviornment: old gp env
        self.oldhome   = None     # Old gphome/bin directory
        self.option    = None     # Argument passed to cmd
        self.path      = None     # Default path
        self.pool      = None     # worker pool
        self.skipcheckcat = False
        self.user      = None     # db admin user
        self.workdir   = None     # directory: masterdir/gpmigrator
        self.checkonly = False    # checkcat only; do not run upgrade

    #------------------------------------------------------------
    def RunCmd(self, cmd, env=None, utility=False, supressDebug=False):
        '''
        Runs a single command on this machine
        '''

        if type(cmd) in [type('string'), type(u'unicode')]:
            cmdstr = cmd
            cmd    = cmd.split(' ')
        elif type(cmd) == type(['list']):
            cmdstr = ' '.join(cmd)
        else:
            logger.warn("Unknown RunCmd datatype '%s'" % str(type(cmd)))
            cmdstr = str(cmd)
        
        if self.debug and not supressDebug: 
            logger.debug("ENV: " + str(env))
            logger.debug("CMD: " + cmdstr)

        # If utility mode has been specified add role to env
        if env == None:
            env = {'PATH': self.path}

        if utility:
            env['PGOPTIONS'] = '-c gp_session_role=utility'

        try:
            pipe = subprocess.Popen(cmd, env=env, 
                                    stdout=subprocess.PIPE, 
                                    stderr=subprocess.PIPE,
                                    close_fds=True)
            result  = pipe.communicate();
        except OSError, e:
            raise CmdError(cmdstr, '', str(e))

        if self.debug and not supressDebug:
            logger.debug(result[1])

        if pipe.returncode:
            raise CmdError(' '.join(cmd), result[0], result[1])
        else:
            return result[0].strip();


    #------------------------------------------------------------
    def SetupEnv(self, gphome, masterdir):
        '''
        Sets up environment variables for Greenplum Administration
        '''
        home  = os.environ.get('HOME')
        lpath = os.environ.get('LD_LIBRARY_PATH')
        dypath = os.environ.get('DYLD_LIBRARY_PATH')

        # Add $GPHOME/bin to the path for this environment
        path = '%s/bin:%s/ext/python/bin:%s' % (gphome, gphome, self.path)

        if lpath:
            lpath = '%s/lib:%s/ext/python/lib:%s' % (gphome, gphome, lpath)
        else:
            lpath = '%s/lib:%s/ext/python/lib' % (gphome, gphome)
        if dypath:
            dypath = '%s/lib:%s/ext/python/lib:%s' % (gphome, gphome, dypath)
        else:
            dypath = '%s/lib:%s/ext/python/lib' % (gphome, gphome)

        env = {}
        env['HOME']    = home
        env['USER']    = self.user
        env['LOGNAME'] = self.user
        env['GPHOME']  = gphome
        env['PATH']    = path
        env['LD_LIBRARY_PATH'] = lpath
        env['DYLD_LIBRARY_PATH'] = dypath
        env['PYTHONPATH'] = os.path.join(gphome, 'lib', 'python')
        env['PYTHONHOME'] = os.path.join(gphome, 'ext', 'python')
        if masterdir:
            env['MASTER_DATA_DIRECTORY'] = masterdir
            env['PGPORT'] = str(self.masterport)
        return env


    #------------------------------------------------------------
    def Select(self, qry, db='template1', port=None, forceutility=False, forceUseEnvUser=False):
        # forceUseEnvUser: user will be GPMIGRATOR if we're in lockdown mode(gpmigrator); otherwise
        #                  it'll be the env user. Setting forceuseEnvUser will always use env user,
        #                  even though we're in gpmigrator and in lockdown.
        '''
        Execute a SQL query and return an array of result rows
           + Single columns will be returned as an array of values
           + Multiple columns will be returned as an array of tuples
        '''

        #
        #   If we are executing from one of the segments then we don't
        #   actually know where the data directory is for the segment
        #   is so we can't check if we are in lockdown mode
        #
        #   Fortunately we never actually lock the segments, so we can
        #   always run as current user.
        #
        if self.cmd == 'MASTER':
            if not self.dbup:
                raise UpgradeError('database is down')
            [env, utility] = self.dbup

            locked = os.path.join(env['MASTER_DATA_DIRECTORY'],
                                  'pg_hba.conf'+LOCKEXT)
            if os.path.exists(locked) and not forceUseEnvUser:
                user = MIGRATIONUSER
            else:
                user = env['USER']

        else:

            # If we lockdown on segments this needs to change, some method
            # would need to be made to determine if the segment in question
            # is locked
            user = self.user
            utility = True

        if port == None:
            port = self.masterport
        elif port != self.masterport:
            utility = True

        if utility or forceutility:
            options = '-c gp_session_role=utility'
        else:
            options = ''

        if self.__dict__.get('sock_dir'):
            sockdir = self.sock_dir
        else:
            sockdir = '/tmp/'
        conn = make_conn(self, user, db, options, port, sockdir)

        curs = None
        logger.debug('executing query ' + qry)
        try:
            curs = conn.query(qry)
        except Exception, e:
            conn.close()
            logger.fatal('Error executing SQL')
            raise ConnectionError('%s\nsql> %s' % (str(e), qry))

        rows = []
        for tuple in curs.dictresult():
            if len(tuple) == 1: 
                rows.append(tuple.values()[0])
            else: 
                rows.append(tuple)
        conn.close()
        return rows
    
    #------------------------------------------------------------
    def Update(self, qry, db='template1', port=None, forceutility=False, upgradeMode=False, modSysTbl=False, forceUseEnvUser=False):
        # forceUseEnvUser: user will be GPMIGRATOR if we're in lockdown mode(gpmigrator); otherwise
        #                  it'll be the env user. Setting forceuseEnvUser will always use env user,
        #                  even though we're in gpmigrator and in lockdown.

        '''
        Execute a SQL query expecting no output
        '''

        #   If we are executing from one of the segments then we don't
        #   have the current database state, so we have to know what to
        #   do via other means.
        #
        #   The assumption is:
        #      A) utility = true
        #      B) pguser = MIGRATIONUSER
        #
        #   If we ever need to run as the non-upgrade user from a segment
        #   this code will obviously need to change
        #
        if self.cmd == 'MASTER':
            if not self.dbup:
                raise UpgradeError('database is down')
            [env, utility] = self.dbup

            locked = os.path.join(env['MASTER_DATA_DIRECTORY'], 
                                  'pg_hba.conf'+LOCKEXT)
            if os.path.exists(locked) and not forceUseEnvUser:
                user = MIGRATIONUSER
            else:
                user = env['USER']
        else:

            # If we lockdown on segments this needs to change, some method
            # would need to be made to determine if the segment in question
            # is locked
            user = self.user
            utility = True

        if port == None:
            port = self.masterport
        
        options = ' '
        if modSysTbl:               options += ' -c allow_system_table_mods=dml'
        if utility or forceutility: options += ' -c gp_session_role=utility'          
        if upgradeMode:             options += ' -c gp_maintenance_conn=true'

        if self.__dict__.get('sock_dir'):
            sockdir = self.sock_dir
        else:
            sockdir = '/tmp/'
        conn = make_conn(self, user, db, options, port, sockdir)

        logger.debug('executing query ' + qry)
        try:
            conn.query(qry)
        except Exception, e:
            logger.fatal('Error executing SQL')
            raise ConnectionError('%s\nsql> %s' % (str(e), qry))

        finally:
            conn.close()

    #------------------------------------------------------------
    def CheckUp(self, tryconn = False):
        '''
        Checks if one of the postmasters is up and running.
        Return the environment of the running server
        '''
        olddir = self.oldenv['MASTER_DATA_DIRECTORY']
        newdir = self.newenv['MASTER_DATA_DIRECTORY']
        env = None

        if olddir and os.path.exists('%s/postmaster.pid' % olddir):
            logger.debug('CheckUp: found old postmaster running')
            env = self.oldenv
        if newdir and os.path.exists('%s/postmaster.pid' % newdir):
            logger.debug('CheckUp: found new postmaster running')
            env = self.newenv

        if not env:
            logger.info('Postmaster not running')
            raise UpgradeError("Postmaster failed to start")

        if tryconn:
            try:
                logger.debug('trying to establish connection to database')
                logger.debug('user = %s, db = template1, port = %i, dir = %s'
                    % (self.user, self.masterport, self.sock_dir))
                conn = make_conn(self, self.user, 'template1', '', self.masterport,
                                 self.sock_dir)
            except Exception:
                raise UpgradeError("Found a postmaster.pid but postmaster " +
                                   "running")
            conn.close()
        return env

    #------------------------------------------------------------
    def CheckDown(self, warn=False):
        '''
        Checks that neither postmaster is running and that the database 
        was cleanly shutdown.
        '''
        if self.cmd == 'MASTER':
            datadirs = [ self.masterdir ]
        else:
            datadirs = self.datadirs

        shutdown_re = re.compile('Database cluster state: *(.*)')
            
        for oldseg in datadirs:
            (d, content) = os.path.split(oldseg)
            newseg = os.path.join(d, WORKDIR, UPGRADEDIR, content)
 
            # Only check the upgrade directory if its not legacy
            if self.datadirs:
                dirs = [oldseg, newseg]
            else:
                dirs = [oldseg]

            for dir in dirs:
                if dir == oldseg:  env = self.oldenv
                else:              env = self.newenv

                # upgrade directory might not actually exist
                if not os.path.isdir(dir):
                    continue

                pid = os.path.join(dir, 'postmaster.pid')
                if os.path.exists(pid):
                    raise UpgradeError("Greenplum process running: " + pid)

                shutdown = self.RunCmd('pg_controldata ' + dir, env=env)
                for line in shutdown.split('\n'):
                    m = shutdown_re.match(line)
                    if m:
                        if m.group(1) == 'shut down': 
                            break
                        msg  = 'pg_controldata: "Database cluster state: %s"\n' % m.group(1)
                        msg += 'Greenplum segment %s did not shutdown cleanly' % dir
                        if warn:
                            logger.warn(msg)
                        else:
                            raise UpgradeError(msg)
            
        if self.cmd == 'MASTER' and self.datadirs:
            self.CallSlaves('CHKDOWN')
            
        return True

    #------------------------------------------------------------
    def Startup(self, env, utility=False, upgrade=False):
        '''
        Starts up the specified database
        '''

        if self.dbup:
            raise UpgradeError('database already started')
        self.dbup = [env, utility]

        isu = ""
        if utility:
            isu = " in utility mode"
        elif upgrade:
            isu = " in upgrade mode"

        if (env == self.oldenv):
            logger.info('Starting old Greenplum postmaster%s' % isu)
        else:
            logger.info('Starting new Greenplum postmaster%s' % isu)

        try:
            cmd = "gpstart -a"
            if utility:
                cmd = cmd +' -m'
                env['GPSTART_INTERNAL_MASTER_ONLY'] = '1'

            if upgrade:  cmd = cmd +' -U upgrade'

            cmd += ' -l %s' % self.logdir

            locked = os.path.join(env['MASTER_DATA_DIRECTORY'],
                                  'pg_hba.conf'+LOCKEXT)
            try:
                if os.path.exists(locked):
                    env['PGUSER'] = MIGRATIONUSER
                    logger.debug("lockfile: '%s' exists" % locked)
                else:
                    logger.debug("lockfile: '%s' does not exist" % locked)

                logger.debug("Starting cluster with env = %s" % str(env))
                pid = subprocess.Popen(cmd, preexec_fn=os.setpgrp,
                                       env=env, shell=True,
                                       stdout=self.logfile, 
                                       stderr=self.logfile, 
                                       close_fds=True)
            finally:
                if os.path.exists(locked):
                    del env['PGUSER']

            # Ignore interrupt requests until startup is done
            error = None
            retcode = None
            while retcode == None:
                try: 
                    retcode = pid.wait();

                except KeyboardInterrupt, e:
                    if not self.interrupted:
                        logger.fatal('***************************************')
                        logger.fatal('SIGINT-: Upgrade Aborted')
                        logger.fatal('***************************************')
                        logger.info( 'Performing clean abort')
                        self.interrupted = True
                        error = e
                    else:
                        logger.info( 'SIGINT-: Still processing shutdown')

            if retcode < 0:
                raise UpgradeError("Startup terminated by signal");

            today = date.today().strftime('%Y%m%d')
            logname = os.path.join(self.logdir, 'gpstart_%s.log' % today)
            if retcode == 1:
                logger.warn('***************************************')
                logger.warn('Warnings generated starting cluster')
                logger.warn('Check %s for detailed warnings' % logname)
                logger.warn('***************************************')
            if retcode > 1:
                logger.fatal('***************************************')
                logger.fatal('Startup failed with error code %d' % retcode)
                logger.fatal('Check %s for detailed warnings' % logname)
                logger.fatal('***************************************')
                raise UpgradeError('Startup failed')

            # If we recieved an interrupt, resignal it now that the startup is done
            if error: 
                raise error

        except OSError, e:
            logger.fatal(str(e))
            raise UpgradeError('Startup failed')

        self.CheckUp();
        
        
    #------------------------------------------------------------
    def Shutdown(self):
        '''
        Stops the specified database
        '''
        if not self.dbup:
            return
        [env, utility] = self.dbup

        dir = env['MASTER_DATA_DIRECTORY']
        if not os.path.exists('%s/postmaster.pid' % dir):
            logger.warn(
                     'Shutdown skipped - %s/postmaster.pid not found' % dir)
            return

        if (env == self.oldenv):
            logger.info('Shutting down old Greenplum postmaster')
        else:
            logger.info('Shutting down new Greenplum postmaster')

        locked = os.path.join(env['MASTER_DATA_DIRECTORY'],
                              'pg_hba.conf'+LOCKEXT)

        try:

            # Note on arguments to gpstop:
            #   This code has gone back and forth on -s vs -f for shutdown:
            #
            #   -f aborts active connections.  This is a good thing.  If 
            #      a user or script snuck in a connection before we were able
            #      to establish the lockdown then we want to abort that 
            #      connection otherwise it will cause the upgrade to fail and 
            #      that is bad.
            #
            #      Prior versions would sometimes issue a kill for fast 
            #      shutdown, this is a problem since we need the database 
            #      shutdown cleanly with no pending xlog transactions.
            #      Because of that we switched to -s.
            # 
            #   -s causes problems because it is a stop that will fail if a 
            #      session is connected and we want to abort active sessions.
            #      Because of that we switched back to -f.
            #
            #   The belief is currently that the current version of gpstop
            #   should be good with passing -f.  To help safeguard this belief
            #   there is a check when we set the catalog version to ensure 
            #   that the database shutdown cleanly.
            #   
            # If this needs to be changed again please read the above,
            # consider what happens if you try to upgrade with an active 
            # connection open to the database, and procede cautiously.

            if utility:  cmd = 'gpstop -a -f -m'
            else:        cmd = 'gpstop -a -f'

            cmd += ' -l %s' % self.logdir

            locked = os.path.join(env['MASTER_DATA_DIRECTORY'],
                                  'pg_hba.conf'+LOCKEXT)
            try:
                if os.path.exists(locked):
                    env['PGUSER'] = MIGRATIONUSER
                    logger.debug('handling locked shutdown')
                logger.debug('shutting down with env: %s' % str(env))
                logger.debug('shutting down with command: %s' % cmd)

                pid = subprocess.Popen(cmd, preexec_fn=os.setpgrp,
                                       env=env, shell=True,
                                       stdout=self.logfile, 
                                       stderr=self.logfile,
                                       close_fds=True)
            finally:
                if os.path.exists(locked):
                    del env['PGUSER']
            self.dbup = None

            # Ignore interrupt requests until shutdown is done
            error = None
            retcode = None
            while retcode == None:
                try: 
                    retcode = pid.wait();

                except KeyboardInterrupt, e:
                    if not self.interrupted:
                        logger.fatal('***************************************')
                        logger.fatal('SIGINT-: Upgrade Aborted')
                        logger.fatal('***************************************')
                        logger.info( 'Performing clean abort')
                        self.interrupted = True
                        error = e
                    else:
                        logger.info( 'SIGINT-: Still processing shutdown')

            if retcode < 0:
                raise UpgradeError("Shutdown terminated by signal");
            today = date.today().strftime('%Y%m%d')
            logname = os.path.join(self.logdir, 'gpstop_%s.log' % today)
            if retcode == 1:
                logger.warn('***************************************')
                logger.warn('Warnings generated stopping cluster')
                logger.warn('Check %s for detailed warnings' % logname)
                logger.warn('***************************************')
            if retcode > 1:
                logger.fatal('***************************************')
                logger.fatal('Shutdown failed with error code %d' % retcode)
                logger.fatal('Check %s for detailed error' % logname)
                logger.fatal('***************************************')
                raise UpgradeError('Shutdown failed')

            # If we recieved an interrupt, resignal it now that the startup is done
            if error: 
                raise error

        except OSError, e:
            logger.fatal(str(e))
            raise UpgradeError('Shutdown failed')

        self.CheckDown();
        

    #------------------------------------------------------------
    def PreUpgradeCheck(self):
        '''
        Check master/segment binary version, GUC, free space, catalog
        '''
        self.CheckBinaryVersion()
        self.CheckGUCs()
        self.CheckFreeSpace()
        if not self.skipcheckcat:
            self.CheckCatalog()
        else:
            logger.info('Skip Checking Catalog')

    #------------------------------------------------------------
    def CheckBinaryVersion(self):
        '''
        Validate that the correct binary is installed in all segments
        '''
        logger.info('Checking Segment binary version')
        hosts = self.Select("select distinct hostname from gp_segment_configuration");
        masterversion = self.getversion(self.newhome, self.newenv)

        cmdStr = '%s/bin/pg_ctl --gp-version' % self.newhome
        for uh in hosts:
            cmd = base.Command(uh, cmdStr, base.REMOTE, uh)
            self.pool.addCommand(cmd)
        self.pool.join()
        items = self.pool.getCompletedItems()
        for i in items:
            if i.results.rc:
                logger.error("error on host %s with error: %s" % (i.remoteHost, i.results.stderr))
                raise UpgradeError('Cannot verify segment GPDB binary')
            if not i.results.stdout:
                logger.error("could not find version string from host %s with command: %s" % (i.remoteHost, cmdStr))
                raise UpgradeError('Cannot verify segment GPDB binary')
            version_string = i.results.stdout.strip()
            if version_string != masterversion:
                logger.error("version string on host %s: '%s' does not match expected: '%s'" % (i.remoteHost, version_string, masterversion))
                raise UpgradeError('Master/Segment binary mismatch')

    #------------------------------------------------------------
    def CheckGUCs(self):
        '''
        Validate that the GUCs are set properly
        - gp_external_enable_exec must be set to true
        '''
        logger.info('Checking GUCs')
        disable_exec_ext = self.Select("select current_setting('gp_external_enable_exec')");
        if (disable_exec_ext[0] == 'off'):
            logger.fatal('***************************************')
            logger.fatal("gp_external_enable_exec is set to 'false'.")
            logger.fatal("Please set gp_external_enable_exec to 'true'.")
            logger.fatal('***************************************')
            raise UpgradeError('Invalid GUC value')

    #------------------------------------------------------------
    def CheckFreeSpace(self):
        '''
        Validate that we've enough space for upgrade.
        Right now, we require 2GB. It's arbitrary.
        '''
        logger.info('Checking Free Space')
        nospacesegs = self.Select("select dfsegment from gp_toolkit.gp_disk_free where dfspace::float/1024/1024 < 2")
        if (len(nospacesegs)>0):
            logger.fatal('***************************************')
            logger.fatal('The following segment ID has less than 2GB of free space.')
            logger.fatal('Please make sure that there is at least 2GB of free space on each segment before upgrade.')
            logger.fatal(nospacesegs)
            logger.fatal('***************************************')
            raise UpgradeError('Insufficient Space on Segment')

        df=unix.DiskFree.get_disk_free_info_local('gpmigrator_check_freespace', self.masterdir)
        mdf = float(df[3])/1024/1024
        if (mdf < 2):
            logger.fatal('***************************************')
            logger.fatal('The Master data directory has only %sGB of free space.' % mdf)
            logger.fatal('Please make sure that there is at least 2GB of free space on the master.')
            logger.fatal('***************************************')
            raise UpgradeError('Insufficient Space on Master')
        
    #------------------------------------------------------------
    def CheckCatalog(self):
        '''
        Validate that the created catalog looks good
        '''
        logger.info('Checking Catalog')
        self.CheckUp()
        [env, utility] = self.dbup

        locked = os.path.join(env['MASTER_DATA_DIRECTORY'],
                              'pg_hba.conf'+LOCKEXT)
        if os.path.exists(locked):
            user = MIGRATIONUSER
        else:
            user = env['USER']

        exec_cmd = libdir + '/gpcheckcat -p %d -U %s -B %i ' % \
            (self.masterport, user, PARALLELISM)
        
        oids = sorted(self.dbs.keys())
        for dboid in oids:
            db = self.dbs[dboid]
                
            if db == 'template0':
                continue
            
            cmd = exec_cmd + db
            logger.info('... Checking ' + db)

            logger.debug("CMD: " + cmd)

            outfilename = os.path.join(self.workdir, 'gpcheckcat_%s.log' % db)
            outfilename = outfilename.replace(' ', '_')
            outfile = open(outfilename, 'w')

            pipe = subprocess.Popen(cmd, shell=True, stdout=outfile, stderr=outfile,close_fds=True)
            retcode = pipe.wait()

            if retcode < 0:
                raise UpgradeError('Catalog Check terminated by signal')
            if retcode > 0:
                raise UpgradeError('Catalog Check Failed - see %s for details' % outfilename)
            
            
    #------------------------------------------------------------
    def PerformPostUpgrade(self):
        '''
        Handles various post upgrade tasks including:
          - Populates the gp_toolkit schema
          - Performs updates for the gpperfmon database
        '''
        try:

            # Get the admin role and all the databases
            logger.info("Installing gp_toolkit")
            rolname = self.Select("select rolname from pg_authid where oid=10")[0]

            # Read the toolkit sql file into memory
            fname = '%s/share/postgresql/gp_toolkit.sql' % self.newhome
            sql = open(fname, 'r').read()

            # Set our role to the admin role then execute the sql script
            sql = "SET SESSION AUTHORIZATION %s;\n%s" % (rolname, sql)
            oids = sorted(self.dbs.keys())
            for dboid in oids:
                db = self.dbs[dboid]

                if db == 'template0':
                    continue
                self.Update(sql, db=db)

        except BaseException, e:
            sys.stderr.write(traceback.format_exc())
            sys.stderr.write(str(e))
            raise e

    #------------------------------------------------------------
    def getversion(self, home,env):
        binary = os.path.join(home, 'bin', 'pg_ctl')
        if not os.path.exists(binary):
            raise UpgradeError(binary + ' not found')

        try:
            return self.RunCmd('pg_ctl --gp-version', env=env)
        except CmdError:
            pass
        conf = '%s/include/pg_config.h' % home
        if os.path.exists(conf):
            cmd = 'grep PG_VERSION_STR ' + conf
            try:
                return self.RunCmd(cmd, env=self.oldenv)
            except Exception, e:
                logger.fatal(str(e))
        raise UpgradeError('Unable to determine version of %s' % home)

