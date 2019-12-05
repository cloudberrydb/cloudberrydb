-- start_ignore
CREATE LANGUAGE plpythonu;
-- end_ignore

DO LANGUAGE plpythonu $$
    import os
    import sys
    import glob
    import shutil
    import subprocess

    if sys.platform not in ('linux', 'linux2'):
        # packcore only works on linux
        return

    # generate and verify a packcore tarball
    #
    # TODO: packcore can list shared libraries with gdb, ldd, or ld-linux.so,
    # we should verify all of them, but so far there is no cmdline option to
    # specify it.  although we could rename the commands to fallback to others,
    # we should not do it, it requires root permission and might corrupt the
    # developer system.  on concourse, gdb is not installed by default, so the
    # gdb way is not covered by the pipelines.
    def test_packcore(cmds):
        # cleanup old files and dirs
        shutil.rmtree(tarball, ignore_errors=True)
        shutil.rmtree(dirname, ignore_errors=True)

        # generate the tarball, the packcore command should return 0
        subprocess.check_call(cmds)
        assert os.path.isfile(tarball)

        # extract the tarball
        subprocess.check_call(['tar', '-zxf', tarball])
        assert os.path.isdir(dirname)

        # verify that binary and shared libraries are included
        assert os.path.exists('{}/postgres'.format(dirname))
        assert os.path.exists('{}/lib64/ld-linux-x86-64.so.2'.format(dirname))

        if os.path.exists('/usr/bin/gdb'):
            # load the coredump and run some simple gdb commands
            os.chdir(dirname)
            subprocess.check_call(['./runGDB.sh',
                                   '--batch',
                                   '--nx',
                                   '--eval-command=bt',
                                   '--eval-command=p main',
                                   '--eval-command=p fork',
                                   '--eval-command=p MyProcPid'])
            os.chdir('..')

    # gzip runs much faster with -1
    os.putenv('GZIP', '-1')

    gphome = os.getenv('GPHOME')
    assert gphome

    postgres = '{}/bin/postgres'.format(gphome)
    assert os.path.exists(postgres)

    packcore = '{}/sbin/packcore'.format(gphome)
    assert os.path.exists(packcore)

    # 'packcore --help' should return 0
    subprocess.check_call([packcore, '--help'])
    subprocess.check_call([packcore, '-h'])

    # 'packcore --version' should return 0
    subprocess.check_call([packcore, '--version'])

    cores = glob.glob('/tmp/core.*')
    if not cores:
        # no coredump found, skip the packcore tests
        return

    corefile = cores[0]
    corename = os.path.basename(corefile)
    tarball = 'packcore-{}.tgz'.format(corename)
    dirname = 'packcore-{}'.format(corename)

    # 'packcore core' should work
    test_packcore([packcore,
                   corefile])

    # 'packcore -b postgres core' should work
    test_packcore([packcore,
                   '--binary={}'.format(postgres),
                   corefile])
    test_packcore([packcore,
                   '--binary', postgres,
                   corefile])
    test_packcore([packcore,
                   '-b', postgres,
                   corefile])
$$;
-- vi: sw=4 et :
