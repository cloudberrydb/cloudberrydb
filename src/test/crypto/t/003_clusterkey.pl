# Test cluster file encryption key managment
#

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

if ($ENV{with_ssl} eq 'openssl')
{
	plan tests => 6;
}
else
{
	plan skip_all => "tests cannot run without OpenSSL";
}

# generate two cluster file encryption keys of random hex digits
my ($rand_hex, $rand_hex2);
$rand_hex  .= sprintf("%x", rand 16) for 1 .. 64;
$rand_hex2 .= sprintf("%x", rand 16) for 1 .. 64;

# initialize cluster using the first cluster key
my $node = get_new_node('node');
$node->init(
	extra => [
		'--file-encryption-method', 'AES256',
		'--cluster-key-command',    "echo $rand_hex"
	]);

# Set wal_level to 'replica';  encryption can't use 'minimal'
$node->append_conf('postgresql.conf', 'wal_level=replica');

$node->start;

# check encryption method
my $file_encryption_method =
  $node->safe_psql('postgres', 'SHOW file_encryption_method;');
ok($file_encryption_method eq 'AES256', 'file_encryption_method is valid');

# record pg_proc count
my $old_pg_proc_count =
  $node->safe_psql('postgres', 'SELECT COUNT(*) FROM pg_proc;');
ok($old_pg_proc_count > 0, 'pg_proc count is valid');

# create permanent table
$node->safe_psql('postgres',
	'CREATE TABLE perm_table (x) AS SELECT * FROM generate_series(1, 100);');

# create unlogged table
# Non-permanent tables like unlogged tables use a special nonce bit, so test those here.
$node->safe_psql('postgres',
	'CREATE UNLOGGED TABLE unlog_table (x) AS SELECT * FROM generate_series(1, 200);'
);

# We can run pg_alterckey and change the cluster_key_command here
# without affecting the running server.
system_or_bail(
	'pg_alterckey',
	"echo $rand_hex",
	"echo $rand_hex2",
	$node->data_dir);

$node->safe_psql('postgres',
	"ALTER SYSTEM SET cluster_key_command TO 'echo $rand_hex2'");

$node->stop;

# start/stop with new cluster key
$node->start;

# check encryption method
$file_encryption_method =
  $node->safe_psql('postgres', 'SHOW file_encryption_method;');
ok($file_encryption_method eq 'AES256', 'file_encryption_method is valid');

# check pg_proc count
my $new_pg_proc_count =
  $node->safe_psql('postgres', 'SELECT COUNT(*) FROM pg_proc;');
ok($new_pg_proc_count == $old_pg_proc_count, 'old/new pg_proc counts match');

# check permanent table count
my $perm_table_count =
  $node->safe_psql('postgres', 'SELECT COUNT(*) FROM perm_table;');
ok($perm_table_count == 100, 'perm_table_count count matches');

# check unlogged table count
my $unlog_table_count =
  $node->safe_psql('postgres', 'SELECT COUNT(*) FROM unlog_table;');
ok($unlog_table_count == 200, 'unlog_table_count count matches');

$node->stop;
