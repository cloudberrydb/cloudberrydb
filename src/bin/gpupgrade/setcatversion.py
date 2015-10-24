import subprocess

releases = {"3.0": "200703112",
     "3.1": "200712072",
     "3.2": "200808253",
     "3.3": "200902041"}


def release2catverno(rno):
     if not rno in releases.keys():
          raise Exception("unknown version %s" % rno)
     return releases[rno]

def stop_cluster():
     p.subprocess.Popen(['gpstop', '-a'], shell=False, close_fds=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
     result = p.communicate()

     if p.returncode != 0:
          raise Exception("could not stop cluster: " + result[0] + result[1])

def get_control_data(datadir):
     '''
     Parse the output of pg_controldata run on data directory, returning
     catalog version and state
     '''
     cmd = ['pg_controldata', datadir]
     p = subprocess.Popen(cmd, shell=False, close_fds=True,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
     result = p.communicate()

     if p.returncode != 0:
          raise Exception("error running " + ' '.join(cmd) + ": " + result[0] + result[1])

     out = result[0].strip()
     ver = ""
     state = ""
     for line in out.split('\n'):
          s = line.split(':')
          if s[0] == 'Catalog version number':
                ver = s[1].strip()
          elif s[0] == 'Database cluster state':
                state = s[1].strip()

     return [ver, state]

def setcatversion(datadir, frm, to):
    '''
    Set catalog version to 'to' from 'frm'. Check that the system is down
    and actually set to the previous version.
    '''
    (ver, state) = get_control_data(datadir)

    frmcatverno = release2catverno(frm)
    if ver != frmcatverno:
         raise Exception("Expected version %s but found %s" % (frmcatverno, ver))

    cmd = ['/Users/swm/greenplum-db-devel/bin/lib/gpmodcatversion', '--catversion', to, datadir]
    p = subprocess.Popen(cmd,
                         shell=False, close_fds=True,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result = p.communicate()
    if p.returncode != 0:
        raise Exception("could not update catalog to %s" % to)

if __name__ == '__main__':
    paths = ['/Users/swm/greenplum-db-devel/upg/upgradetest-1',
             '/Users/swm/greenplum-db-devel/upg/upgradetest1',
             '/Users/swm/greenplum-db-devel/upg/upgradetest0']
    for p in paths:
        setcatversion(p, '3.2', '3.3')
