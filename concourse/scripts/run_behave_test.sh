#!/bin/bash
set -ex

BEHAVE_FLAGS=$@

function setup_coverage() {
    # Enables coverage.py on all hosts in the cluster. Note that this function
    # modifies greenplum_path.sh, so callers need to source that file AFTER this
    # is done.
    local commit_sha
    read -r commit_sha < /home/gpadmin/gpdb_src/.git/HEAD
    local coverage_path="/tmp/coverage/$commit_sha"

    # This file will be copied into GPDB's PYTHONPATH; it sets up the coverage
    # hook for all Python source files that are executed.
    cat > /tmp/sitecustomize.py <<SITEEOF
import coverage
coverage.process_startup()
SITEEOF

    # Set up coverage.py to handle analysis from multiple parallel processes.
    cat > /tmp/coveragerc <<COVEOF
[run]
branch = True
data_file = $coverage_path/coverage
parallel = True
COVEOF

    # Now copy everything over to the hosts.
    while read -r host; do
        scp /tmp/sitecustomize.py "$host":/usr/local/greenplum-db-devel/lib/python
        scp /tmp/coveragerc "$host":/usr/local/greenplum-db-devel
        ssh "$host" "mkdir -p $coverage_path" < /dev/null

        # Enable coverage instrumentation after sourcing greenplum_path.
        ssh "$host" "echo 'export COVERAGE_PROCESS_START=/usr/local/greenplum-db-devel/coveragerc' >> /usr/local/greenplum-db-devel/greenplum_path.sh" < /dev/null
    done < /tmp/hostfile_all
}

# virtualenv 16.0 and greater does not support python2.6, which is
# used on centos6
pip install --user virtualenv~=15.0
export PATH=$PATH:~/.local/bin

# create virtualenv before sourcing greenplum_path since greenplum_path
# modifies PYTHONHOME and PYTHONPATH
#
# XXX Patch up the vendored Python's RPATH so we can successfully run
# virtualenv. If we instead set LD_LIBRARY_PATH (as greenplum_path.sh does), the
# system Python and the vendored Python will collide and virtualenv will fail.
# This step requires patchelf.
patchelf \
    --set-rpath /usr/local/greenplum-db-devel/ext/python/lib \
    /usr/local/greenplum-db-devel/ext/python/bin/python

virtualenv \
    --python /usr/local/greenplum-db-devel/ext/python/bin/python /tmp/venv

# Install requirements into the vendored Python stack on all hosts.
mkdir -p /tmp/py-requirements
source /tmp/venv/bin/activate
    pip install --prefix /tmp/py-requirements -r /home/gpadmin/gpdb_src/gpMgmt/requirements-dev.txt
    while read -r host; do
        rsync -rz /tmp/py-requirements/ "$host":/usr/local/greenplum-db-devel/ext/python/
    done < /tmp/hostfile_all
deactivate

# Enable coverage.py on all hosts. (This modifies greenplum_path.sh and must
# come before the source below.)
setup_coverage

cat > ~/gpdb-env.sh << EOF
  source /usr/local/greenplum-db-devel/greenplum_path.sh
  export PGPORT=5432
  export MASTER_DATA_DIRECTORY=/data/gpdata/master/gpseg-1
  export PGDATABASE=gptest

  alias mdd='cd \$MASTER_DATA_DIRECTORY'
EOF
source ~/gpdb-env.sh

if gpstate > /dev/null 2>&1 ; then
  createdb gptest
  gpconfig --skipvalidation -c fsync -v off
  gpstop -u
fi

cd /home/gpadmin/gpdb_src/gpMgmt
make -f Makefile.behave behave flags="$BEHAVE_FLAGS"
