use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 11;

program_help_ok('droplang');
program_version_ok('droplang');
program_options_handling_ok('droplang');

my $node = get_new_node('main');
$node->init;
$node->start;

# GPDB: drop gp_toolkit before trying to drop plpgsql in tests; otherwise they
# fail because of the dependency.
$node->safe_psql('postgres', 'DROP SCHEMA gp_toolkit CASCADE');

$node->issues_sql_like(
	[ 'droplang', 'plpgsql', 'postgres' ],
	qr/statement: DROP EXTENSION "plpgsql"/,
	'SQL DROP EXTENSION run');

$node->command_fails(
	[ 'droplang', 'nonexistent', 'postgres' ],
	'fails with nonexistent language');
