# Reads and parses .rsp files and runs them through testcrypto
#
# Test vectors downloaded from:
#
# https://csrc.nist.gov/projects/cryptographic-algorithm-validation-program/cavp-testing-block-cipher-modes
#
# Specifically GCM Test Vectors (SP 800-38D):
#
# https://csrc.nist.gov/CSRC/media/Projects/Cryptographic-Algorithm-Validation-Program/documents/mac/gcmtestvectors.zip
#
# Note that the AADlen > 0 cases were removed from our set, since we don't support that and
# the test code will just skip them currently, and we also don't bother testing 192-bit,
# but one could download the full set and run all of the files through if they wished and
# all should pass (they did when this test suite was written originally).

use strict;
use warnings;
use TestLib;
use Test::More;

if ($ENV{with_ssl} eq 'openssl')
{
	plan tests => 56;
}
else
{
	plan skip_all => 'SSL not supported by this build';
}


# XXX add CTR tests here

my $algo     = "AES-GCM";
my @rspfiles = (
	"gcmDecrypt128.rsp",      "gcmDecrypt256.rsp",
	"gcmEncryptExtIV128.rsp", "gcmEncryptExtIV256.rsp");

note "running tests";

foreach my $rspfile (@rspfiles)
{
	open(my $in_rspfile, '<', $rspfile) || die;
	my %testrun;
	my %lengths;

	while (my $line = <$in_rspfile>)
	{

		chomp($line);

		# Remove CR, if it's there.
		$line =~ s/\r$//;

		# Skip comments
		if ($line =~ /^[[:space:]]*#/) { next; }

		# If we hit a blank, time to run a test
		if ($line =~ /^[[:space:]]*$/)
		{
			if (%testrun)
			{
				my @testargs;

				# Set up the command to run
				push(@testargs, ("$ENV{TESTDIR}/testcrypto", '-a', $algo));

				if ($testrun{'Key'})
				{
					push(@testargs, ('-k', $testrun{'Key'}));
				}

				if ($testrun{'IV'})
				{
					push(@testargs, ('-i', $testrun{'IV'}));
				}

				if ($testrun{'CT'})
				{
					push(@testargs, ('-c', $testrun{'CT'}));
				}

				if ($testrun{'AAD'})
				{
					# Don't currently support AAD
					undef(%testrun);
					next;
				}

				if ($testrun{'Tag'})
				{
					push(@testargs, ('-t', $testrun{'Tag'}));
				}

				if ($testrun{'PT'})
				{
					push(@testargs, ('-p', $testrun{'PT'}));
				}

				if ($testrun{fail})
				{
					command_exit_is(\@testargs, 1,
						"Run $testrun{Count} of Keylen: $lengths{Keylen}, IVlen: $lengths{IVlen}, PTlen: $lengths{PTlen}, AADlen: $lengths{AADlen}, Taglen: $lengths{Taglen}"
					);
				}
				else
				{
					command_ok(\@testargs,
						"Run $testrun{Count} of Keylen: $lengths{Keylen}, IVlen: $lengths{IVlen}, PTlen: $lengths{PTlen}, AADlen: $lengths{AADlen}, Taglen: $lengths{Taglen}"
					);
				}
				undef(%testrun);
				undef(%lengths);
			}
			else
			{
				next;
			}
		}

		# Grab length information, just to have.
		if ($line =~ /^\[([A-Za-z]*) = ([0-9]*)]$/)
		{
			$lengths{$1} = $2;
			next;
		}

		if ($line =~ /^([A-Za-z]*) = ([a-f0-9]*)$/)
		{
			$testrun{$1} = $2;
		}

		if ($line =~ /^FAIL$/)
		{
			$testrun{fail} = 1;
		}
	}
}
