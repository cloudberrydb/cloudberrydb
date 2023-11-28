#!/usr/bin/perl
#----------------------------------------------------------------------
#
# genbki.pl
#    Perl script that generates postgres.bki and symbol definition
#    headers from specially formatted header files and data files.
#    postgres.bki is used to initialize the postgres template database.
#
# Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/backend/catalog/genbki.pl
#
#----------------------------------------------------------------------

use strict;
use warnings;
use Getopt::Long;

use FindBin;
use lib $FindBin::RealBin;
use 5.010;

my $pg_amproc_key = "pg_amproc";
my $pg_amop_key = "pg_amop";
my $pg_proc_key = "pg_proc";

my %catalog_data;

foreach my $input_file (@ARGV)
{
	$input_file =~ /(\w+)\.dat$/
		or die "Input files need to be .dat files.\n";
	my $catname = $1;
	open(my $ifd, '<', $input_file) || die "$input_file: $!";
	my %data;

	# Scan the input file.
	my $count = 0;
	while (<$ifd>)
	{
		my $hash_ref;

		if (/{/)
		{
			my $lcnt = tr/{//;
			my $rcnt = tr/}//;

			if ($lcnt == $rcnt)
			{
				# We're treating the input line as a piece of Perl, so we
				# need to use string eval here. Tell perlcritic we know what
				# we're doing.
				eval '$hash_ref = ' . $_;   ## no critic (ProhibitStringyEval)
				if (!ref $hash_ref)
				{
					die "$input_file: error parsing line $.:\n$_\n";
				}

				if ($catname eq "pg_operator")
				{
					if (!exists($hash_ref->{oprleft}))
					{
						say $hash_ref->{oid};
					}
					my $t_key = $hash_ref->{oprname} . "(";
					if ($hash_ref->{oprleft} ne "0")
					{
						$t_key .= $hash_ref->{oprleft} . ",";
					}
					$t_key .= $hash_ref->{oprright} . ")";
					if (exists($data{$t_key})) {
						say $t_key;
					} else {
						if ($count == 0) {
							say $t_key;
						}
						$data{$t_key} = 1;
						$count = $count + 1;
					}

				}
				else
				{
					die "$input_file: error read data $.:\n$catname\n";
				}
			}
			else
			{
				my $next_line = <$ifd>;
				die "$input_file: file ends within Perl hash\n"
					if !defined $next_line;
				$_ .= $next_line;
				redo;
			}
		}
	}
	close $ifd;
	say $count;
}