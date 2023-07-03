# Test cluster file encryption buffer management

# This tests that an encrypted server actually encrypts the database
# files.  It does this by checking for strings in the database files in
# both non-encrypted and encrypted clusters.  We test a system table, a
# permanent relation, and a unlogged/non-permanent table.
# (Non-permanent relations use a special nonce bit, which is why we test
# it here.)

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

if ($ENV{with_ssl} eq 'openssl')
{
	plan tests => 17;
}
else
{
	plan skip_all => "tests cannot run without OpenSSL";
}

my %file_match_count;

sub get_cluster_file_contents
{
	my $node = shift();
	my %relnode;

	# get postgres database oid
	my $postgres_db_oid = $node->safe_psql('postgres',
		"SELECT oid FROM pg_database WHERE datname = 'postgres';");
	ok($postgres_db_oid != 0, 'retrieving postgres database oid');

	# get pg_proc relfilenode
	$relnode{pg_proc} =
	  $node->safe_psql('postgres', "SELECT pg_relation_filenode('pg_proc');");
	ok($relnode{pg_proc} != 0, 'retrieving pg_proc relfilenode');

	# create permanent table
	$node->safe_psql('postgres',
		"CREATE TABLE perm_table (x) AS SELECT 'aaaaaaaa' FROM generate_series(1, 100);"
	);

	# get permanent table relfilenode
	$relnode{perm} = $node->safe_psql('postgres',
		"SELECT pg_relation_filenode('perm_table');");
	ok($relnode{perm} != 0, 'retrieving permanent table relfilenode');

	# create unlogged table
	$node->safe_psql('postgres',
		"CREATE UNLOGGED TABLE unlog_table (x) AS SELECT 'bbbbbbbb' FROM generate_series(1, 200);"
	);

	# get unlogged table relfilenode
	$relnode{unlog} = $node->safe_psql('postgres',
		"SELECT pg_relation_filenode('unlog_table');");
	ok($relnode{unlog} != 0, 'retrieving unlogged table relfilenode');

	my $file_contents =
	  slurp_file($node->basedir .
		  '/pgdata/base/' . $postgres_db_oid . '/' . $relnode{pg_proc});
	# () converts to list context
	$file_match_count{pg_proc} = () = $file_contents =~ m/pg_[a-z]{3,}/g;

	$file_contents =
	  slurp_file($node->basedir .
		  '/pgdata/base/' . $postgres_db_oid . '/' . $relnode{perm});
	$file_match_count{perm} = () = $file_contents =~ m/a{8,}/g;

	$file_contents =
	  slurp_file($node->basedir .
		  '/pgdata/base/' . $postgres_db_oid . '/' . $relnode{unlog});
	$file_match_count{unlog} = () = $file_contents =~ m/b{8,}/g;
}

#
# Test with disabled encryption
#

# initialize cluster
my $non_encrypted_node = get_new_node('non_encrypted_node');
$non_encrypted_node->init();

$non_encrypted_node->start;

# check encryption is disabled
my $file_encryption_method =
  $non_encrypted_node->safe_psql('postgres', 'SHOW file_encryption_method;');
ok($file_encryption_method eq '', 'file_encryption_method is valid');

get_cluster_file_contents($non_encrypted_node);

# record pg_proc count
my $query_pg_proc_count = $non_encrypted_node->safe_psql('postgres',
	"SELECT COUNT(*) FROM pg_proc WHERE proname ~ '^pg_[a-z]{3,}';");
ok($query_pg_proc_count > 0, 'pg_proc count is valid');

# check pg_proc count
ok($file_match_count{pg_proc} != $query_pg_proc_count,
	'SQL/file pg_proc counts match');

# check permanent table count
ok($file_match_count{perm} != 100, 'perm_table count matches');

# check unlogged table count
ok($file_match_count{unlog} != 200, 'unlog_table count matches');

$non_encrypted_node->stop;


#---------------------------------------------------------------------------

#
# Test with enabled encryption
#

my $rand_hex;
$rand_hex .= sprintf("%x", rand 16) for 1 .. 64;

# initialize cluster using a cluster key
my $encrypted_node = get_new_node('encrypted_node');
$encrypted_node->init(
	extra => [
		# We tested AES256 in 003, so use AES128
		'--file-encryption-method', 'AES128',
		'--cluster-key-command',    "echo $rand_hex"
	]);

# Set wal_level to 'replica';  encryption can't use 'minimal'
$encrypted_node->append_conf('postgresql.conf', 'wal_level=replica');

$encrypted_node->start;

# check encryption method
$file_encryption_method =
  $encrypted_node->safe_psql('postgres', 'SHOW file_encryption_method;');
ok($file_encryption_method eq 'AES128', 'file_encryption_method is valid');

get_cluster_file_contents($encrypted_node);

# Because the files are encrypted, we should get zero matches for all
# comparisons below.  However, technically the encrypted data might
# match the desired string, so we allow one such match.

# check pg_proc
ok($file_match_count{pg_proc} <= 1, 'pg_proc is encrypted');

# check permanent table
ok($file_match_count{perm} <= 1, 'perm_table is encrypted');

# check unlogged table
ok($file_match_count{unlog} <= 1, 'unlog_table is encrypted');

$encrypted_node->stop;
