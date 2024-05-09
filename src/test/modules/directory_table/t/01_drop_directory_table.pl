
#  Copyright (c) 2024, Cloudberry Database, HashData Technology Limited.

# Test drop directory table.
#
# This test can only run with Unix-domain sockets.

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;
if (!$use_unix_sockets)
{
	plan skip_all =>
	  "drop directory table tests cannot run without Unix-domain sockets";
}
else
{
	plan tests => 10;
}

# Delete pg_hba.conf from the given node, add a new entry to it
# and then execute a reload to refresh it.
sub reset_pg_hba
{
	my $node       = shift;
	my $hba_method = shift;

	unlink($node->data_dir . '/pg_hba.conf');
	$node->append_conf('pg_hba.conf', "local all all $hba_method");
	$node->reload;
	return;
}

# Initialize primary node. Force UTF-8 encoding, so that we can use non-ASCII
# characters in the passwords below.
my $node = get_new_node('primary');
my ($ret, $stdout, $stderr);
$node->init(extra => [ '--locale=C', '--encoding=UTF8' ]);
$node->append_conf('postgresql.conf', "log_connections = on\n");
$node->start;

# Create test directory table.
$node->safe_psql(
    'postgres',
    "CREATE DIRECTORY TABLE test_dir1;
CREATE DIRECTORY TABLE test_dir2;
");

# Create test roles.
$node->safe_psql(
	'postgres',
	"SET client_encoding='utf8';
CREATE USER test_user1;
CREATE USER test_user2 SUPERUSER;
");

# Test CREATE DIRECTORY TABLE
($ret, $stdout, $stderr) =
$node->role_psql(
    'test_user1',
    'postgres',
    "CREATE DIRECTORY TABLE test_dir3;"
);
is($ret, 0, 'create directory table succeed');

($ret, $stdout, $stderr) =
$node->role_psql(
    'test_user2',
    'postgres',
    "CREATE DIRECTORY TABLE test_dir4;"
);
is($ret, 0, 'create directory table succeed');

# Test DROP DIRECTORY TABLE
($ret, $stdout, $stderr) =
$node->role_psql(
    'test_user1',
    'postgres',
    "DROP DIRECTORY TABLE test_dir1;"
);
is($ret, 3, 'user has no privileges to drop directory table');
like(
    $stderr,
    qr/must be owner of directory table test_dir1/,
    'expected error from user can not drop directory table'
);

($ret, $stdout, $stderr) =
$node->role_psql(
    'test_user2',
    'postgres',
    "DROP DIRECTORY TABLE test_dir2;"
);
is($ret, 0, 'drop directory table succeed');

($ret, $stdout, $stderr) =
$node->role_psql(
    'test_user1',
    'postgres',
    "DROP DIRECTORY TABLE test_dir3;"
);
is($ret, 0, 'drop directory table succeed');

($ret, $stdout, $stderr) =
$node->role_psql(
    'test_user1',
    'postgres',
    "DROP DIRECTORY TABLE test_dir4;"
);
is($ret, 3, 'user has no privileges to drop directory table');
like(
    $stderr,
    qr/must be owner of directory table test_dir4/,
    'expected error from user can not drop directory table'
);

($ret, $stdout, $stderr) =
$node->role_psql(
    'test_user2',
    'postgres',
    "DROP DIRECTORY TABLE test_dir4;"
);
is($ret, 0, 'drop directory table succeed');

($ret, $stdout, $stderr) =
$node->role_psql(
    'test_user2',
    'postgres',
    "DROP DIRECTORY TABLE test_dir1;"
);
is($ret, 0, 'drop directory table succeed');

# cleanup
reset_pg_hba($node, 'trust');
$node->safe_psql(
    'postgres',
    "DROP USER test_user1;
    DROP USER test_user2;
");