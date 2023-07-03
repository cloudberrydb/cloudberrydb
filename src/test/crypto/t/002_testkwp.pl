# Reads and parses .rsp files and runs them through testcrypto
#
# (Partial) Test vectors downloaded from:
#
# https://csrc.nist.gov/projects/cryptographic-algorithm-validation-program/cavp-testing-block-cipher-modes
#
# Specifically Key Wrap Test Vectors (SP 800-38F):
#
# https://csrc.nist.gov/CSRC/media/Projects/Cryptographic-Algorithm-Validation-Program/documents/mac/kwtestvectors.zip
#
# We don't include the 192-bit tests, though they all worked when this test suite was written.
# We also don't include the _inv tests as those aren't supported in OpenSSL yet.

use strict;
use warnings;
use TestLib;
use Test::More;

if ($ENV{with_ssl} eq 'openssl')
{
	plan tests => 20;
}
else
{
	plan skip_all => 'SSL not supported by this build';
}


my $algo;
# my @txtfiles = ("KWP_AD_128.txt", "KWP_AD_192.txt", "KWP_AD_256.txt", "KWP_AE_128.txt", "KWP_AE_192.txt", "KWP_AE_256.txt");
my @txtfiles =
  ("KWP_AD_128.txt", "KWP_AD_256.txt", "KWP_AE_128.txt", "KWP_AE_256.txt");

note "running tests";

foreach my $txtfile (@txtfiles)
{
	open(my $in_txtfile, '<', $txtfile) || die;
	my %testrun;
	my %lengths;

	if ($txtfile =~ /^KWP_/)
	{
		$algo = 'AES-KWP';
	}

	while (my $line = <$in_txtfile>)
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

				if ($testrun{'K'})
				{
					push(@testargs, ('-k', $testrun{'K'}));
				}

				if ($testrun{'C'})
				{
					push(@testargs, ('-c', $testrun{'C'}));
				}

				if ($testrun{'P'})
				{
					push(@testargs, ('-p', $testrun{'P'}));
				}

				if ($testrun{fail})
				{
					command_exit_is(\@testargs, 1,
						"Run $testrun{COUNT} of Plaintext Length: $lengths{'PLAINTEXT LENGTH'}"
					);
				}
				else
				{
					command_ok(\@testargs,
						"Run $testrun{COUNT} of Plaintext Length: $lengths{'PLAINTEXT LENGTH'}"
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
		if ($line =~ /^\[([A-Za-z ]*) = ([0-9]*)]$/)
		{
			$lengths{$1} = $2;
			next;
		}

		if ($line =~ /^([A-Z]) = ([a-f0-9]*)$/)
		{
			$testrun{$1} = $2;
		}

		if ($line =~ /^COUNT = ([0-9]*)$/)
		{
			$testrun{COUNT} = $1;
		}

		if ($line =~ /^FAIL$/)
		{
			$testrun{fail} = 1;
		}
	}
}
