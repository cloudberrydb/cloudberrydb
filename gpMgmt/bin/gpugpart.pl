#!/usr/bin/env perl
#
#
# copyright (c) 2008, 2009
# Author: Jeffrey I Cohen
#
#
use Pod::Usage;
use Getopt::Long;
use Data::Dumper;
use Time::Local;
use strict;
use warnings;

=head1 NAME

B<gpugpart.pl> - GreenPlum UpGrade Partitioned Tables

=head1 SYNOPSIS

B<gpugpart.pl> [options] 

Options:

    -help            brief help message
    -man             full documentation
    -connect         psql connect parameters
    -sql             generate sql to construct new partition definitions
	-table           only upgrade the specified table(s)
	-exclude-table   exclude specified table(s) from upgrade

=head1 OPTIONS

=over 8

=item B<-help>

    Print a brief help message and exits.

=item B<-man>
    
    Prints the manual page and exits.

=item B<-connect>
    
    psql connect string, e.g:

    -connect '-p 11000 -d template1'

=item B<-sql>
    
If no arguments are specified, generate sql to create new partition definitions and transfer the contents (via INSERT...SELECT) of the old partitions. Valid arguments are a comma-separated list of the following:

=over 12

=item B<CREATE> 

create new partition definitions.  The new definitions have a suffix to distinguish them from the original tables.

=item B<DEFAULT> 

Add default partitions to the new partitioned tables.

=item B<TRANSFER> 

transfer data from old partitions to new partitions using INSERT...SELECT


=item B<EXCHANGE> 

transfer data from old partitions using EXCHANGE (note that TRANSFER and EXCHANGE are mutually-exclusive)


=item B<CLEANUP> 

rename the original tables with a "old" suffix and replace them with the new partitioned tables.


=back

    An example argument is: --sql create,exchange,cleanup

which generates a script to create the new partition definitions, exchanges the contents, and renames the new partition tables to replace the originals.

=item B<-table> <tablename>
    
Only upgrade the specified table(s).  Multiple tables may be
specified, eg:

	-table schema1.foo -table schema2.bar -table schema3.baz

In addition, the SQL LIKE wildcards "%" and "_" may be used in the
table names.  Note that if the schema name is not specified, the
assumption is to search all schemas.  Similarly, to upgrade all of the
tables in a particular schema, use:

	-table schemaname.%

=item B<-exclude-table> <tablename>
    
Do not upgrade the specified table(s).  Multiple tables may be
specified, eg:

	-exclude-table schema1.foo -exclude-table schema2.bar 

In addition, the SQL LIKE wildcards "%" and "_" may be used in the
table names.  Note that if the schema name is not specified, the
assumption is to search all schemas.  Similarly, to exclude all of the
tables in a particular schema, use:

	-exclude-table schemaname.%

=back

=head1 DESCRIPTION

gpugpart.pl generates sql scripts which upgrade "rule-based" partition
tables (from the gpcreatepart script, 3.1 and prior) to 3.2 enhanced
partitioned tables, using CREATE and ALTER TABLE syntax.  The enhanced
partitioned tables have many improvements, including catalog support,
fast INSERT/COPY, better query plans, easier manageability, etc.  

=head1 LIMITATIONS

The sql scripts construct enhanced partitioned tables of identical
schema to their "rule-based" counterparts, but they does not preserve
additional characteristics like granted permissions, etc.  The
generated scripts should be reviewed and modified as necessary by DBAs
who are familiar with the existing schemas.


=head1 AUTHORS

Jeffrey I Cohen

Copyright (c) 2008, 2009 GreenPlum.  All rights reserved.  

Address bug reports and comments to: bugs@greenplum.org


=cut

my $glob_id = "";
my $glob_connect;
my $glob_dosql;
my $glob_table_include;
my $glob_table_exclude;


sub sql_compare_like
{
	# adapted from Genezzo::Havok::SQLCompare
    my ($pattern, $escape) = @_;

    return undef
        unless (defined($pattern));

	if ($pattern !~ m/\./)
	{
		# prefix with wildcard for schema name
		$pattern = '%.' . $pattern;
	}

    $pattern = '^' . quotemeta($pattern) . '$';

    my $wildcard = '.*';
    my $singlechar = '.';

    if (defined($escape))
    {
        return undef
            unless (length($escape) > 0);

        $escape = quotemeta($escape);

        # zero width negative look behind -- match any occurence of
        # "%" wildcard which does not follow the escape character (and
        # similarly for "_")
        $pattern =~ s/(?<!$escape)\\%/$wildcard/gm;
        $pattern =~ s/(?<!$escape)_/$singlechar/gm;

        # replace the "escaped" match expressions with their literal
        # values
        $pattern =~ s/(($escape)\\%)/\\%/gm;
        $pattern =~ s/(($escape)_)/_/gm;
    }
    else
    {
        $pattern =~ s/\\%/$wildcard/gm;
        $pattern =~ s/_/$singlechar/gm;
    }

	return ($pattern);
}

BEGIN {
    my $man  = 0;
    my $help = 0;
    my $conn;
    my $dosql;
	my @table_only;
	my @table_exclude;

    GetOptions(
               'help|?' => \$help, man => \$man, 
               "connect=s" => \$conn,
               "sql:s" => \$dosql,
		       "table=s" => \@table_only, # t
		       "excludetable|exclude-table=s" => \@table_exclude # T
               )
        or pod2usage(2);

    
    pod2usage(-msg => $glob_id, -exitstatus => 1) if $help;
    pod2usage(-msg => $glob_id, -exitstatus => 0, -verbose => 2) if $man;

    $glob_connect = $conn;

    $glob_connect = '-p 11000 -d template1'
        unless (defined($glob_connect));

    if (defined($dosql))
    {
        if (!length($dosql))
        {
            $glob_dosql = ["CREATE", "XFR"];
        }
        else
        {
            my $crt = 0;
            my $xfr = 0;
            my $xch = 0;
            my $cln = 0;
            my $mdf = 0;

            $dosql =~ s/\s//gm;

            my @foo = split(/\,/, $dosql);

            for my $opt (@foo)
            {
                unless ($opt =~ m/^(no)?(create|crt|xch|exchange|default|xfr|transfer|cln|clean)/i)
                {
                    $glob_id = "invalid sql option: $opt\n";
                    pod2usage(-msg => $glob_id, -exitstatus => 1);
                    exit();
                }

                if ($opt =~ m/^(no)?(create|crt)/i)
                {
                    $crt = !($opt =~ m/^no/i);
                }
                if ($opt =~ m/^(no)?(xch|exchange)/i)
                {
                    $xch = !($opt =~ m/^no/i);
                }
                if ($opt =~ m/^(no)?(xfr|transfer)/i)
                {
                    $xfr = !($opt =~ m/^no/i);
                }
                if ($opt =~ m/^(no)?(cln|clean)/i)
                {
                    $cln = !($opt =~ m/^no/i);
                }
                if ($opt =~ m/^(no)?(default)/i)
                {
                    $mdf = !($opt =~ m/^no/i);
                }

            } # end for

            if ($xfr && $xch)
            {
                $glob_id = "invalid sql option: cannot combine EXCHANGE and TRANSFER\n";
                pod2usage(-msg => $glob_id, -exitstatus => 1);
                exit();

            }

            $glob_dosql = [];

            push @{$glob_dosql}, "CREATE" if ($crt);
            push @{$glob_dosql}, "MDF" if ($mdf);
            push @{$glob_dosql}, "XFR" if ($xfr);
            push @{$glob_dosql}, "XCH" if ($xch);
            push @{$glob_dosql}, "CLN" if ($cln);

        }

		my (@t1, @t2);

		# build perl pattern expressions for table names
		for my $tname (@table_only)
		{
			push @t1, sql_compare_like($tname);
		}
		for my $tname (@table_exclude)
		{
			push @t2, sql_compare_like($tname);
		}

		$glob_table_include = \@t1
			if (scalar(@t1));
		$glob_table_exclude = \@t2
			if (scalar(@t2));

    } # end if dosql

}

# convert a postgresql psql formatted table into an array of hashes
sub tablelizer
{
    my $ini = shift;

    # first, split into separate lines, the find all the column headings

    my @lines = split(/\n/, $ini);

    return undef
        unless (scalar(@lines));

    my $line1 = shift @lines;

    # look for <space>|<space>
    my @colheads = split(/\s+\|\s+/, $line1);

    # fixup first, last column head (remove leading,trailing spaces)

    $colheads[0] =~ s/^\s+//;
    $colheads[0] =~ s/\s+$//;
    $colheads[-1] =~ s/^\s+//;
    $colheads[-1] =~ s/\s+$//;

    return undef
        unless (scalar(@lines));
    
    shift @lines; # skip dashed separator

    my @rows;

    for my $lin (@lines)
    {
        my @cols = split(/\|/, $lin, scalar(@colheads));
        last 
            unless (scalar(@cols) == scalar(@colheads));

        my $rowh = {};

        for my $colhd (@colheads)
        {
            my $rawcol = shift @cols;

            $rawcol =~ s/^\s+//;
            $rawcol =~ s/\s+$//;

            $rowh->{$colhd} = $rawcol;
        }
        push @rows, $rowh;
    }

    return \@rows;
}


sub infix_proc_fn
{
    my ($biga, $rex) = @_;

    my $outi = [];
                        
    while (scalar(@{$biga}))
    {
        my $curr1 = shift @{$biga};

        $curr1 = paren_fixup($curr1);

        if ($curr1 =~ m/$rex/)
        {
            my $h1 = {};

            $h1->{val} = $curr1;
            $h1->{left} = pop @{$outi};

            my $curr2 = shift @{$biga};
            $curr2 = paren_fixup($curr2);

            $h1->{right} = $curr2;

            push @{$outi}, $h1;                                
        }
        else
        {
            push @{$outi}, $curr1;
        }
    }

    return $outi;
}

# process infix expressions
sub infix_proc
{
    my $biga = shift;

    my @f2;

    for my $vv3 (@{$biga})
    {
        push @f2, $vv3
            if  (defined($vv3) && length($vv3) && ($vv3 !~ m/^\s+$/));
    }

    my $biga2 = infix_proc_fn(\@f2, qr/^(\<|\>|\=|\<\>|\<\=|\>\=|\+|\-|\%|\/)$/ );
    my $biga3 = infix_proc_fn($biga2, qr/^AND$/i );
    my $biga4 = infix_proc_fn($biga3, qr/^OR$/i );

    return $biga4;
}

sub paren_fixup
{
    my $foo = shift;
    
    return $foo
        unless ((ref($foo) eq 'HASH') && (exists($foo->{biga})));

    my $biga = $foo->{biga};

    my $biga2 = infix_proc($biga);

    # reduced to a single val
    return $biga2->[0]
        if (1 == scalar(@{$biga2}));

    $foo->{biga} = $biga2;

    return $foo;
}

sub paren_proc_fn
{
    my ($biga, $level) = @_;

    my $outi = [];

    while (scalar(@{$biga}))
    {
        my $tok = shift @{$biga};

        if (defined($tok))
        {
            return $outi
                if ($tok =~ m/^\)$/);

            if ($tok =~ m/^\($/)
            {
                my $foo  = paren_proc_fn($biga, $level + 1);

                $tok = {};
                $tok->{biga} = $foo;
            }
        }

        push @{$outi}, $tok;
    }

    return $outi;
}

sub paren_proc
{
    my $biga = shift;

    my $biga2 = [];

    for my $vv3 (@{$biga})
    {
        push @{$biga2}, $vv3
            if  (defined($vv3) && length($vv3) && ($vv3 !~ m/^\s+$/));
    }


    my $outi = paren_proc_fn($biga2, 0);

    return $outi;
}

# parse words
sub pw1
{
    my $txt = shift;
                    
    use Text::ParseWords;

    $txt =~ s/^\s+//;
    $txt =~ s/\s+$//;

    my @wfoo = parse_line('[()\s+]', 'delimiters', $txt);
#                        my @wfoo = parse_line('\s+', 0, $txt);
#    print Data::Dumper->Dump(\@wfoo);


    my $biga  = paren_proc(\@wfoo);
    my $biga2 = infix_proc($biga);

    # reduced to a single val
    return $biga2->[0]
        if (1 == scalar(@{$biga2}));

    return $biga2;
}

# make partition specs
sub makespec
{
    my ($pspec, $pps) = @_;

    if ($pps->{typ} !~ /range|list/i)
    {
        print STDERR "invalid partition type: $pps->{typ}\n";
        return undef;
    }

    # build the start and end for a RANGE
    if ($pps->{typ} =~ m/range/i)
    {
        $pspec->{start} = {};
        $pspec->{end} = {};

        if (exists($pps->{range_start}))
        {
            my @foo = keys(%{$pps->{range_start}->{colh}});

            $pspec->{start}->{keys} = [];
            push (@{$pspec->{start}->{keys}}, @foo);

            $pspec->{start}->{values} = [];

            for my $jjj (@foo)
            {
                push @{$pspec->{start}->{values}}, 
                @{$pps->{range_start}->{colh}->{$jjj}};
            }


            @foo = keys(%{$pps->{range_start}->{colh_oper}});

            # is inclusive if comparison operator includes "="
            $pspec->{start}->{inclusive} = 0 +
                ($pps->{range_start}->{colh_oper}->{$foo[0]}->[0] =~ m/\=/);

        }
        if (exists($pps->{range_end}))
        {
            my @foo = keys(%{$pps->{range_end}->{colh}});

            $pspec->{end}->{keys} = [];
            push (@{$pspec->{end}->{keys}}, @foo);

            $pspec->{end}->{values} = [];

            for my $jjj (@foo)
            {
                push @{$pspec->{end}->{values}}, 
                @{$pps->{range_end}->{colh}->{$jjj}};
            }

            @foo = keys(%{$pps->{range_end}->{colh_oper}});

            # is inclusive if comparison operator includes "="
            $pspec->{end}->{inclusive} = 0 + 
                ($pps->{range_end}->{colh_oper}->{$foo[0]}->[0] =~ m/\=/);

        }

    }

    # build keys for LIST
    if ($pps->{typ} =~ m/list/i)
    {
        $pspec->{start} = {};

        my @foo = keys(%{$pps->{colh}});

        $pspec->{start}->{keys} = [];
        push (@{$pspec->{start}->{keys}}, @foo);

        $pspec->{start}->{values} = [];
        
        my $maxlen = 0;

        # make sure have same number of key column values...
        for my $jjj (@foo)
        {
            my $l2 = scalar(@{$pps->{colh}->{$jjj}});

            if ($maxlen == 0)
            {
                $maxlen = $l2;
            }
            else
            {
                if ($l2 != $maxlen)
                { 
                    print STDERR "missing key columns: ",
                    Data::Dumper->Dump([$pps]), "\n";
                }
            }
        }

        for my $ii (0..($maxlen-1))
        {

            # interleave the key values in order, 
            # ie for columns a,b,c
            # do values (a1,b1,c1,a2,b2,c2...)
            for my $jjj (@foo)
            {
                push @{$pspec->{start}->{values}}, 
                $pps->{colh}->{$jjj}->[$ii];
            }
        }

    }

    return $pspec;
} # end makespec

# genpartdefs: generate partition definitions.  Pass 1 - generate the
# specs and sort the range partitions.
sub genpartdefs
{
    my $reg_tabs = shift;

    while (my ($kk, $vv) = each (%{$reg_tabs}))
    {

#        print "kk: $kk\n";

#        print Data::Dumper->Dump([$vv]), "\n";
        
        next unless (exists($vv->{rules}));

        my $ruleh = $vv->{rules};

        while (my ($kk2, $vv2) = each (%{$ruleh}))
        {
            
#            print "  rule: $kk2\n";
#            print "    parent: $vv2->{parent}\n";
#            print "    child:  $vv2->{child}\n";

#            print Data::Dumper->Dump([$vv2]), "\n";

			next
				unless (exists($vv2->{parsetree}));

			unless (ref($vv2->{parsetree}) eq "HASH")
			{
				print "weirdo: $kk\n";
				print Data::Dumper->Dump([$vv2]),"\n";
				print "skipping: $kk\n";
				next;
			}

			next unless (exists($vv2->{parsetree}->{partSpec}));

            my $pps = $vv2->{parsetree}->{partSpec};

            if (exists($vv->{pby}))
            {
                if ($vv->{pby}->{typ} ne $pps->{typ})
                {
                    print STDERR "partition type mismatch for rule $kk2\n";
                    print STDERR "$vv->{pby}->{typ} vs $pps->{typ}\n";

                }
                else
                {
                    my $pspec = { child => $vv2->{child}, 
								  childrulename => $kk2,
								  childruledef => $vv2->{ruledef}
  };

                    $pspec = makespec($pspec, $pps);

                    push @{$vv->{pby}->{pspec}}, $pspec;
                }
            }
            else
            {
                $vv->{pby} = {typ => $pps->{typ}, 
                              pspec => [] };
 
                my $pspec = { child => $vv2->{child}, 
							  childrulename => $kk2,
							  childruledef => $vv2->{ruledef}
				};
                
                $pspec = makespec($pspec, $pps);

                push @{$vv->{pby}->{pspec}}, $pspec;
               
            }


        } # end while kk2    

        # sort the range partitions if possible
        if (exists($vv->{pby})
            && exists($vv->{pby}->{pspec})
            && ($vv->{pby}->{typ} =~ m/range/i))
        {
            my $ps = $vv->{pby}->{pspec}->[0];
            my $val1;

#            print "\n\n\nsorting???\n\n\n";

            if (defined($ps->{start}->{values}))
            {
                $val1 = $ps->{start}->{values}->[0];
            }
            elsif (defined($ps->{end}->{values}))
            {
                $val1 = $ps->{end}->{values}->[0];
            }
                
            if (defined($val1))
            {

#                print "\n\n\nsorting $val1\n\n\n";

                # numeric only
                if ($val1 =~ m/^\d+$/)
                {
                    my @foo = 
                        sort { $a->{start}->{values}->[0] <=>
                                   $b->{start}->{values}->[0] } 
                    @{$vv->{pby}->{pspec}};

                    $vv->{pby}->{pspec} = \@foo;
                }
                elsif ($val1 =~ m/\:\:date$/)
                {
                    my @foo = 
                        sort { $a->{start}->{values}->[0] cmp
                                   $b->{start}->{values}->[0] } 
                    @{$vv->{pby}->{pspec}};

                    $vv->{pby}->{pspec} = \@foo;
                }
                elsif ($val1 =~ m/\:\:timestamp/)
                {
                    my @foo = 
                        sort { $a->{start}->{values}->[0] cmp
                                   $b->{start}->{values}->[0] } 
                    @{$vv->{pby}->{pspec}};

                    $vv->{pby}->{pspec} = \@foo;
                }

            }

        }

        if (exists($vv->{pby}))
        {
            my $p1 = 1;

            for my $ps (@{$vv->{pby}->{pspec}})
            {
                # make a partition name
                $ps->{name} = "p" . $p1 
                    unless (exists($ps->{name}) &&
                            defined($ps->{name}));
                
                $p1++;
                
                for my $tt (qw(start end))
                {
                    next
                        unless (exists($ps->{$tt}));

#                        print Data::Dumper->Dump([$ps->{$tt}]), "\n";
                    
                    if (exists($vv->{pby}->{keys}))
                    {
                        # test if keys match!!

                        if (defined($ps->{$tt}->{keys}))
                        {
                            if (scalar(@{$vv->{pby}->{keys}})
                                != scalar(@{$ps->{$tt}->{keys}}))
                            {
                                print STDERR scalar(@{$vv->{pby}->{keys}}), " ",
                                scalar(@{$ps->{$tt}->{keys}}) , "\n";

                                print STDERR "key count mismatch for ",
                                "\nps: $tt ", Data::Dumper->Dump([$ps->{$tt}]), "\npby:",
                                Data::Dumper->Dump([$vv->{pby}]), "\n";
                                next;
                            }
                            for my $jjj (0..(scalar(@{$vv->{pby}->{keys}})-1))
                            {
                                if ($vv->{pby}->{keys}->[$jjj] ne
                                    $ps->{$tt}->{keys}->[$jjj])
                                {
                                print STDERR "key name mismatch for ",
                                "\nps: $tt ", Data::Dumper->Dump([$ps->{$tt}]), "\npby:",
                                Data::Dumper->Dump([$vv->{pby}]), "\n";

                                    next;
                                }

                            }

                            delete $ps->{$tt}->{keys};
                        }

                    }
                    else
                    {
                        $vv->{pby}->{keys} = [];
                        
                        push (@{$vv->{pby}->{keys}}, 
                              @{$ps->{$tt}->{keys}})
                            if (defined($ps->{$tt}->{keys}));
                        
                        delete $ps->{$tt}->{keys}
                        if (defined($ps->{$tt}->{keys}));

                    }
                } # end for tt
            }
        } # end if pby


    } # end while kk


} # end genpartdefs

# genpartdefs2: Pass 2 - fixup invalid partition specs, ie both start
# and end point to same value.
sub genpartdefs2
{
    my $reg_tabs = shift;

    my $do_fixup = 1;

    my $magic_interval_fixup;

    while (my ($kk, $vv) = each (%{$reg_tabs}))
    {
        if (exists($vv->{pby}))
        {
            $magic_interval_fixup = undef;

            for my $psc (0..(scalar(@{$vv->{pby}->{pspec}}) - 1))
            {
                my $pspec = $vv->{pby}->{pspec}->[$psc];

                if ($vv->{pby}->{typ} =~ m/range/i)
                {
                    my ($v1, $v2);
                    my ($incex1, $incex2);

                    $v1 = undef;
                    $v2 = undef;

                    $v1 = $pspec->{start}->{values}->[0]
                        if (exists($pspec->{start}));
                    $v2 = $pspec->{end}->{values}->[0]
                        if (exists($pspec->{end}));

                    if (defined($v1))
                    {
                        $incex1 = $pspec->{start}->{inclusive} ?
                            "INCLUSIVE" : "EXCLUSIVE";
                    }

                    if (defined($v2))
                    {
                        $incex2 = $pspec->{end}->{inclusive} ?
                            "INCLUSIVE" : "EXCLUSIVE";

                        # XXX XXX: fixup the dual inclusive case if possible...
                        if ($do_fixup && 
                            ($v1 eq $v2) && ($incex2 eq "INCLUSIVE")
                            && ($incex2 eq $incex1))
                        {
							$pspec->{singleton_range} = 1;

                            if ($v2 =~ m/^\d+$/)
                            {
								# MPP-6662: recalculate the fixup each
								# time to avoid daylight savings time
								# problems for 2006-10-29, 2007-11-04
								# (ie 25 hour days)
                                if (1)
                                {
                                    my $pspec2 = $vv->{pby}->{pspec}->[$psc+1];

                                    if (defined($pspec2))
                                    {
                                        my $v1_1;

                                        $v1_1 = 
                                            $pspec2->{start}->{values}->[0]
                                            if (exists($pspec2->{start}));

                                        $magic_interval_fixup = $v1_1 - $v2;

                                    }
                                }
                                if (defined($magic_interval_fixup))
                                {
                                    $pspec->{end}->{original} 
                                    = {values=>[$v2], inclusive=>1};
                                    $v2 += $magic_interval_fixup;

                                    $pspec->{end}->{values}->[0] = $v2;

                                    $pspec->{end}->{inclusive} = 0;
                                    $incex2 = "EXCLUSIVE";
                                    $pspec->{end}->{magic_interval_fixup} =
                                        $magic_interval_fixup;

									delete $pspec->{singleton_range};
                                }
                            }
                            elsif ($v2 =~ m/\:\:date$/)
                            {
								# MPP-6662: recalculate the fixup each
								# time to avoid daylight savings time
								# problems for 2006-10-29, 2007-11-04
								# (ie 25 hour days)
								if (1)
                                {
                                    my $pspec2 = $vv->{pby}->{pspec}->[$psc+1];

                                    if ($pspec2)
                                    {
                                        my $v1_1;

                                        $v1_1 = 
                                            $pspec2->{start}->{values}->[0]
                                            if (exists($pspec2->{start}));

#                                        $magic_interval_fixup = $v1_1 - $v2;

                                        my @dat2 = 
                                            ($v2 =~ m(^\'(\d\d\d\d)\-(\d\d)\-(\d\d)\'));
                                        my @dat1_1 = 
                                            ($v1_1 =~ m(^\'(\d\d\d\d)\-(\d\d)\-(\d\d)\'));

                                        print STDERR "invalid date: $v2\n"
                                            unless (3 == scalar(@dat2));
                                        print STDERR "invalid date: $v1_1\n"
                                            unless (3 == scalar(@dat1_1));

                                        my $dd1 =
                                          timelocal(0,0,0,
                                                    $dat1_1[2],
                                                    $dat1_1[1] - 1,
                                                    $dat1_1[0]);

                                        my $dd2 = 
                                          timelocal(0,0,0,
                                                    $dat2[2],
                                                    $dat2[1] - 1,
                                                    $dat2[0]);

                                        $magic_interval_fixup = $dd1 - $dd2;


                                    }
                                }
                                if (defined($magic_interval_fixup))
                                {

                                    my @dat2 = 
                                        ($v2 =~ m(^\'(\d\d\d\d)\-(\d\d)\-(\d\d)\'));

                                    $pspec->{end}->{original} 
                                    = {values=>[$v2], inclusive=>1};

                                    my $t2 =
                                        timelocal(0,0,0,
                                                  $dat2[2] ,
                                                  $dat2[1] - 1,
                                                  $dat2[0] )
                                        + $magic_interval_fixup;

                                    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =
                                        localtime($t2);

                                    $year += 1900;
                                    $mon++;

                                    $v2 = sprintf('\'%04d-%02d-%02d\'::date',
                                                  $year, $mon, $mday);

                                    $incex2 = "EXCLUSIVE";
                                    $pspec->{end}->{values}->[0] = $v2;

                                    $pspec->{end}->{inclusive} = 0;
                                    $pspec->{end}->{magic_interval_fixup} =
                                        $magic_interval_fixup;

									delete $pspec->{singleton_range};

#									print Data::Dumper->Dump([$pspec]), "\n";


                                }

                            }
                        }
                    }
                    
                }

            } # end for pspec

        }

    }

} # end genpartdefs2

# genpartdefs3: Pass 3 - add subpartitions to partition by clause
sub genpartdefs3
{
    my $reg_tabs = shift;


    while (my ($kk, $vv) = each (%{$reg_tabs}))
    {
        if (exists($vv->{pby}))
        {
            my $magic_interval_fixup;
            my $good_fixup = 1;

            for my $psc (0..(scalar(@{$vv->{pby}->{pspec}}) - 1))
            {
                my $pspec = $vv->{pby}->{pspec}->[$psc];


                # check for subpartitions
                # NOTE: {child} might be a plain tablename but 
                #       {subpartition} is always schema.tablename
                if (exists($pspec->{child}))
                {
                    my $tname = $pspec->{child};

                    # check for schema
                    if ($tname !~ m/\./)
                    {
                        $tname = $vv->{Schema} . "." . $tname;
                    }

                    if (exists($reg_tabs->{$tname})
                        &&
                        exists($reg_tabs->{$tname}->{pby}))
                    {
                        $pspec->{subpartition} = $tname;

                        # mark as a subpartition so we don't create a
                        # separate partition definition.
                        $reg_tabs->{$tname}->{pby}->{is_subpartition} = 1;

                    }

                }
                

                if ($vv->{pby}->{typ} =~ m/range/i)
                {
                    my ($v1, $v2);
                    my ($incex1, $incex2);

                    $v1 = undef;
                    $v2 = undef;

                    $v1 = $pspec->{start}->{values}->[0]
                        if (exists($pspec->{start}));
                    $v2 = $pspec->{end}->{values}->[0]
                        if (exists($pspec->{end}));

                    if (exists($pspec->{end})
                        && exists($pspec->{end}->{magic_interval_fixup}))
                    {
                        if (defined($magic_interval_fixup))
                        {
                            if ($magic_interval_fixup != 
                                $pspec->{end}->{magic_interval_fixup})
                            {
                                $good_fixup = 0;
                                last;
                            }

                        }
                        else
                        {
                            $magic_interval_fixup = 
                                $pspec->{end}->{magic_interval_fixup};

                        }
                    }
                }
            } # end for my psc

            if (defined($magic_interval_fixup) && $good_fixup)
            {
                $vv->{pby}->{every} = $magic_interval_fixup;
            }
            else
            {
            }


        } # end if exists pby        

    } # end while kk vv

} # end genpartdefs3


sub typename_fixup_in
{
	my $v1 = shift;
	# XXX XXX: with/without time zone fixup
	# MPP-6502, QA-271: fixup types with spaces in name

	$v1 =~ s/timestamp without time zone/timestamp\_without\_time\_zone/gm;
	$v1 =~ s/timestamp with time zone/timestamp\_with\_time\_zone/gm;
	$v1 =~ s/time without time zone/time\_without\_time\_zone/gm;
	$v1 =~ s/time with time zone/time\_with\_time\_zone/gm;

	$v1 =~ s/character varying/character_varying/gm;

	$v1 =~ s/double precision/double_precision/gm;



	return $v1;
}

sub typename_fixup_out
{
	my $v1 = shift;

	$v1 =~ s/timestamp\_without\_time\_zone/timestamp without time zone/gm;
	$v1 =~ s/timestamp\_with\_time\_zone/timestamp with time zone/gm;
	$v1 =~ s/time\_without\_time\_zone/time without time zone/gm;
	$v1 =~ s/time\_with\_time\_zone/time with time zone/gm;

	$v1 =~ s/character_varying/character varying/gm;

	$v1 =~ s/double_precision/double precision/gm;

	return $v1;
}

# genpartdefs4: Pass 4 - build text and comments for partition by clause
sub genpartdefs4
{
    my ($reg_tabs, $all_cons) = @_;

    my $do_fixup = 1;

    my $magic_suffix = "XnewX";
    my $old_suffix = "XoldX";

    while (my ($kk, $vv) = each (%{$reg_tabs}))
    {
        if (exists($vv->{pby}))
        {
            my $newname = $kk . "_" . $magic_suffix;
            my $oldname = $kk . "_" . $old_suffix;
			my $schema = $vv->{Schema};
            my $cln_oldname = $oldname;
            my $cln_name = $kk;

			# MPP-6453: remove schema prefix -- not valid for RENAME
#			print Data::Dumper->Dump([$vv]), "\n";
			$cln_oldname =~ s/^$schema\.//;
			$cln_name    =~ s/^$schema\.//;

            my $cln_txt = "";
            my $xfr_txt = "";
            my $xch_txt = "";

			# use a generic token for default partition name -- fixup
			# to be unique in genpartdefs5
            my $mdf_txt = "ALTER TABLE $newname ADD DEFAULT PARTITION **def** ;\n";

            $xfr_txt .= "INSERT INTO $newname SELECT * FROM $kk;\n";

            $cln_txt .= "ALTER TABLE $kk RENAME TO $cln_oldname;\n";

            if (0)
            {
				$cln_txt .= "-- NOTE: $oldname should contain no rows\n";
				$cln_txt .= "-- if data remains you may need to add default partitions(s) to table $kk\n";
				$cln_txt .= "select count(*) from $oldname; \n";
			}

            $cln_txt .= "ALTER TABLE $newname RENAME TO $cln_name;\n";

            my $ct_txt  = "CREATE TABLE $newname" . " (LIKE " . $kk . ") ";

            my $pby_txt = "PARTITION BY " . uc($vv->{pby}->{typ}) . " " .
                "(" . join(", ", @{$vv->{pby}->{keys}}) . ")";

			my $priv_txt = "";

			if (exists($vv->{owner_privs}))
			{
				$priv_txt .= 
					"ALTER TABLE $newname OWNER TO " . 
					"$vv->{owner_privs}->{owner};\n"
					if (exists($vv->{owner_privs}->{owner}));

				if (exists($vv->{owner_privs}->{grant_regular}))
				{
					my $grant_list = 
						$vv->{owner_privs}->{grant_regular};

					my @kl = sort(keys(%{$grant_list}));

					for my $namekey (@kl)
					{
						my $acla = $grant_list->{$namekey};

						if (scalar(@{$acla}))
						{
							$priv_txt .=
								"\nGRANT \n " . 
								join(", ", @{$acla}) . 
								"\n ON $newname\n" .
								"TO $namekey\;\n";
						}

					}

				}
				if (exists($vv->{owner_privs}->{grant_with_grant}))
				{
					my $grant_list = 
						$vv->{owner_privs}->{grant_with_grant};

					my @kl = sort(keys(%{$grant_list}));

					for my $namekey (@kl)
					{
						my $acla = $grant_list->{$namekey};

						if (scalar(@{$acla}))
						{
							$priv_txt .=
								"\nGRANT \n " . 
								join(", ", @{$acla}) . 
								"\n ON $newname\n" .
								"TO $namekey\n" .
								"WITH GRANT OPTION\;\n";
						}

					}

				}


			}

            my $spec_txt = "(\n";

			# for all partition specs
            for my $psc (0..(scalar(@{$vv->{pby}->{pspec}}) - 1))
            {
                my $pspec = $vv->{pby}->{pspec}->[$psc];

                if (0 != $psc)
                {
					# zero-width negative look-behind assertion: look
					# for "-- NOTE: " that does not follow a comma,
					# and interpose the comma betwixt the partition
					# declaration and the comment
					if ($spec_txt =~ m/(?<!\,)\s+\-\-\s+NOTE\:.*$/)
					{
						$spec_txt =~ s/(?<!\,)(\s+\-\-\s+NOTE\:.*$)/\,$1/;
						$spec_txt .= "\n";
					}
					else
					{
						$spec_txt .= ",\n";
					}
                }
                else
                {
                    if (exists($vv->{pby}->{every}))
                    {
                        $spec_txt .=  '  -- ' . "EVERY $vv->{pby}->{every}" 
                            . "   \n";
                    }
                }

                if (exists($pspec->{child}))
                {
                    # NOTE: these magic comments are used by
                    # genpartdef_subtree_fixup
                    if (exists($pspec->{name}) && defined($pspec->{name}))
                    {
                        $spec_txt .= '  -- ' . $pspec->{name} 
                        . ' derived from: (' ;
                    }
                    else
                    {
                        $spec_txt .= '  -- derived from: (' ;
                    }
                    my $tname = $pspec->{child};

                    # check for schema
                    if ($tname !~ m/\./)
                    {
                        $tname = $vv->{Schema} . "." . $tname;
                    }

                    $spec_txt .=  $tname . ")   \n";

                    if (exists($pspec->{name}) && defined($pspec->{name}))
                    {
						# XXX XXX: identify the table, partition
                        $xch_txt .= "\n-- " . $newname . " " . 
							$pspec->{name};
                        $xch_txt .= "\nALTER TABLE $tname " . 
							" NO INHERIT $kk ; \n";

#						print Data::Dumper->Dump([$all_cons]), "\n";

						# drop all existing constraints on the table
						if (exists($all_cons->{$tname}))
						{
							for my $con_item (@{$all_cons->{$tname}})
							{
								my $cname = $con_item->{name};

								$xch_txt .= "ALTER TABLE $tname " . 
									" DROP CONSTRAINT $cname ; \n";
							}
						}

                        $xch_txt .= "ALTER TABLE $newname " . 
                            "EXCHANGE PARTITION " . $pspec->{name} .
                            " WITH TABLE " . $tname . "; \n";

						# after the exchange, fix the inheritance
                        $xch_txt .= "ALTER TABLE $tname " . 
							" INHERIT $kk ; \n";

						# fixup rule: the rule definition text is
						# correct, but it is associated in the catalog
						# with the relid of the original partition
						# (table).  Drop the rule and recreate it
						# using the newly-exchanged table.

						my $ruledef = "CREATE RULE " . 
							$pspec->{childruledef} . " ; \n";
						my $rulenam = $pspec->{childrulename};

                        $xch_txt .= "\nDROP RULE IF EXISTS $rulenam " .
							"ON $kk ; \n";
                        $xch_txt .= $ruledef . "\n";

						# re-add all existing constraints on the table
						if (exists($all_cons->{$tname}))
						{
							for my $con_item (@{$all_cons->{$tname}})
							{
								my $cname = $con_item->{name};

								$xch_txt .= "ALTER TABLE $tname " . 
									" ADD CONSTRAINT $cname CHECK " .
									$con_item->{src} .  " ; \n";
							}
						}


                    }

                    # find all the tables that are children of this table
                    $vv->{pby}->{children} = []
                        unless (exists($vv->{pby}->{children}));

                    push @{$vv->{pby}->{children}}, $tname;

                    # build up the parent/child tree
                    {
                        if (exists($reg_tabs->{$tname})
                            && exists($reg_tabs->{$tname}->{pby}))
                        {
                            $reg_tabs->{$tname}->{pby}->{parent} = $kk;
                        }

                    }


                }

                $spec_txt .= "  ";
				                
                $spec_txt .= "PARTITION $pspec->{name} "
                    if (exists($pspec->{name}) && defined($pspec->{name}));


                if ($vv->{pby}->{typ} =~ m/range/i)
                {
                    my ($v1, $v2);
                    my ($incex1, $incex2);

                    $v1 = undef;
                    $v2 = undef;

                    $v1 = $pspec->{start}->{values}->[0]
                        if (exists($pspec->{start}));
                    $v2 = $pspec->{end}->{values}->[0]
                        if (exists($pspec->{end}));

                    if (defined($v1))
                    {
						$v1 = typename_fixup_out($v1);

                        $spec_txt .= " START (" .  $v1 . ") ";

                        $incex1 = $pspec->{start}->{inclusive} ?
                            "INCLUSIVE" : "EXCLUSIVE";

                        $spec_txt .= $incex1;
                    }

                    if (defined($v2))
                    {
						$v2 = typename_fixup_out($v2);

                        $incex2 = $pspec->{end}->{inclusive} ?
                            "INCLUSIVE" : "EXCLUSIVE";

                        $spec_txt .=  " END (" .  $v2 . ") ";


                       $spec_txt .= $incex2;
                    }

					# comment on magic_interval_fixup status...
					if (exists($pspec->{singleton_range}))
					{
						$spec_txt .= "  -- NOTE: could not fixup: START = END";
					}
					elsif (exists($pspec->{end}) && 
						   exists($pspec->{end}->{original}))
					{
						$spec_txt .= "  -- NOTE: magic fixup - end was: ";
						$spec_txt .= $pspec->{end}->{original}->{values}->[0];
					}
    
                } # end if type = range


                if ($vv->{pby}->{typ} =~ m/list/i)
                {
                    my $numkeys = scalar(@{$vv->{pby}->{keys}});

                    my @outi;

                    # build the values clause, subgrouping by the key counts

                    if ($numkeys > 1)
                    {
                        my @baz;

						for my $v1 (@{$pspec->{start}->{values}})
						{
							$v1 = typename_fixup_out($v1);
							push @baz, $v1;
						}

                        while (scalar(@baz))
                        {
#                            print "a: ", scalar(@baz), "\n";

							# split array into "numkeys" size pieces
                            my @foo = splice(@baz, 0, $numkeys);

#                            print "b: ", scalar(@baz), "\n";

                            push @outi, "(" . join(", ", @foo) . ")";
                        }
                    }
                    else
                    {
						for my $v1 (@{$pspec->{start}->{values}})
						{
							$v1 = typename_fixup_out($v1);
							push @outi, $v1;
						}
                    }
                    
                    $spec_txt .= "VALUES (" . join(", ", @outi) . ")";
                }

            } # end for pspec

            $spec_txt .= "\n)";

            $vv->{pby}->{ct_txt} =  $ct_txt ;

            $vv->{pby}->{cleanup_txt}  =  $cln_txt ;
            $vv->{pby}->{transfer_txt} =  $xfr_txt ;
            $vv->{pby}->{exchange_txt} =  $xch_txt ;
            $vv->{pby}->{makdeflt_txt} =  $mdf_txt ;

            $vv->{pby}->{pby_txt} =  $pby_txt ;
            $vv->{pby}->{spec_txt} =  $spec_txt ;

            $vv->{pby}->{priv_txt} =  $priv_txt ;

        }

    }

} # end genpartdefs4

sub genpartdef_subtree_fixup
{
    my ($reg_tabs, $pby) = @_;

    my $magic_suffix = "XnewX";

    return
        unless (exists($pby->{children})
                && !exists($pby->{did_subtree_fixup}));

    my $child_pby_txt;

    for my $child (@{$pby->{children}})
    {
        if (exists($reg_tabs->{$child})
            && exists($reg_tabs->{$child}->{pby}))
        {
            # recurse
            genpartdef_subtree_fixup($reg_tabs, 
                                     $reg_tabs->{$child}->{pby});

            if (!defined($child_pby_txt))
            {
                $child_pby_txt = $reg_tabs->{$child}->{pby}->{pby_txt};

                my $c1 = $child_pby_txt;

                $c1 =~ s/^partition/subpartition/i;

                # add the subpartition clause...
                $pby->{pby_txt} .= "\n" . $c1;
            }
            else
            {
                if ($reg_tabs->{$child}->{pby}->{pby_txt} ne
                    $child_pby_txt)
                {
                    print STDERR "subpartition mismatch: ", 
                    Data::Dumper->Dump([$reg_tabs->{$child}->{pby}]), "\n";
                }
            }
            
        } # end if child
        
    } # end for child


    my @foo = split(/\n/, $pby->{spec_txt});

	my %h_parent_def_fixup;

    for my $linnum (0..(scalar(@foo)-1))
    {
        next
            unless ($foo[$linnum] =~ m/^\s+\-\-.*\s+derived from\:/);

        my @baz = 
            ($foo[$linnum] =~ m/^\s+\-\-.*\s+derived from\:\s+\((.*)\)\s+/);
        
        my $tname = shift @baz;

#        print "\n\ngot one!! ($tname)\n\n";

        next unless(exists($reg_tabs->{$tname})
                    && exists($reg_tabs->{$tname}->{pby}));
        
        my $child_spec_txt = $reg_tabs->{$tname}->{pby}->{spec_txt};

        $child_spec_txt =~ s/^(\s+)partition/$1subpartition/igm;

        $child_spec_txt =~ s/^  /    /gm;

        # fix up parens
        $child_spec_txt =~ s/^\(/  \(/;
        $child_spec_txt =~ s/\)$/  \)/;

        my $has_comma = ($foo[$linnum+1] =~ m/\,$/);

        $foo[$linnum+1] =~ s/\,$//; # remove the comma

        $foo[$linnum+1] .= "\n" . $child_spec_txt;

        $foo[$linnum+1] .= "," if ($has_comma);

        if (1)
        {
            # handle exchange in subpartitions, and fixup ADD default
            # partition

            my $child_xch_txt = $reg_tabs->{$tname}->{pby}->{exchange_txt};
            my $child_mdf_txt = $reg_tabs->{$tname}->{pby}->{makdeflt_txt};

            my $parent = $reg_tabs->{$tname}->{pby}->{parent};
            my $pname;

            # find the partition name for the tablename by scanning
            # all the pspecs of the parent.
            for my $psc (@{$reg_tabs->{$parent}->{pby}->{pspec}})
            {
                if ($psc->{subpartition} eq $tname)
                {
                    $pname = $psc->{name};
                    last;
                }
            }

            print STDERR "no partition for $tname\n "
                unless (defined($pname));

            my $t1 = "TABLE $tname"  . "_" . $magic_suffix;
            my $t2 = "TABLE $parent" . "_" . $magic_suffix .
                " ALTER PARTITION $pname";
         
            $child_xch_txt =~ s/$t1/$t2/gm;
            $child_mdf_txt =~ s/$t1/$t2/gm;

            $reg_tabs->{$tname}->{pby}->{exchange_txt} =
                "-- table $tname is partition $pname of $parent\n" . 
                $child_xch_txt;

			$reg_tabs->{$tname}->{pby}->{makdeflt_txt} =  $child_mdf_txt;

			# make the parent default partition have default subpartitions
            my $mdf_txt = $reg_tabs->{$parent}->{pby}->{makdeflt_txt};

			my @mmm;

			# don't bother if already did this for parent
			@mmm = split(/(\*\*def\*\*)/, $mdf_txt)
				unless (exists($h_parent_def_fixup{$parent}));

			$h_parent_def_fixup{$parent} = 1;

			# find the last "**def**", and add a subpartition
			if (scalar(@mmm) > 2)
			{
				my $last_one = $mmm[-2] . $mmm[-1];

				pop @mmm; # remove the last one (**def** plus trailing bits)
				pop @mmm;

#				print "l1: $last_one\n";

				$last_one =~ 
					s/\*\*def\*\*/\*\*def\*\* (default subpartition \*\*def\*\*\)/;

#				print "l2: $last_one\n";

				$reg_tabs->{$parent}->{pby}->{makdeflt_txt} =
					join ("", @mmm) . $last_one;
			}


            unless ($pby->{exchange_txt} =~ m/^\-\-/)
            {
                # comment out exchange for parent if has children
                $pby->{exchange_txt} =~ s/^/\-\- /gm;
                $pby->{exchange_txt} = 
                    "-- exchange occurs in children\n" . $pby->{exchange_txt};

            }

        }
        
        
    }    

    $pby->{spec_txt} = join("\n", @foo);    

    $pby->{did_subtree_fixup} = 1;
}

# genpartdefs5: Pass 5 - hang the subpartition specs in the right places,
# and fixup default parttion names
sub genpartdefs5
{
    my $reg_tabs = shift;

    my $do_fixup = 1;

	my %h_parent_def_fixup;	

    while (my ($kk, $vv) = each (%{$reg_tabs}))
    {
		if (!(defined($glob_table_exclude)) && 
			!(defined($glob_table_include)))
		{
#			print "match0: include $kk \n";
			$vv->{include} = 1;
		}
		else
		{
			if (defined($glob_table_include))
			{
			  l_match1:
				for my $matchup (@{$glob_table_include})
				{
					if ($kk =~ m/$matchup/)
					{
#						print "match1: $kk $matchup \n";

						$vv->{include} = 1;
						last l_match1;
					}
				}
				$vv->{include} = 0
					unless(defined($vv->{include}));

			}
			if (defined($glob_table_exclude))
			{
			  l_match2:
				for my $matchup (@{$glob_table_exclude})
				{
					if ($kk =~ m/$matchup/)
					{
#						print "match2: $kk $matchup \n";

						if (defined($vv->{include}) &&
							(1 == $vv->{include}))
									
						{
							warn "conflict between include and exclude for $kk";						}
						else
						{
							$vv->{include} = 0;
						}
						last l_match2;
					}
				}

				$vv->{include} = 1
					unless(defined($vv->{include}));

			}
		}


        if (exists($vv->{pby}))
        {
            genpartdef_subtree_fixup($reg_tabs, $vv->{pby});

			# XXX XXX: make DEFAULT partition definitions
            my $mdf_txt = $vv->{pby}->{makdeflt_txt};

			next
				unless ($mdf_txt =~ m/ALTER TABLE .* ADD DEFAULT PARTITION/);

			my @foo = 
				($mdf_txt =~ m/ALTER TABLE\s+(\S+)\s+(ALTER PARTITION.*)?ADD DEFAULT PARTITION/);

			next 
				unless (scalar(@foo));

			my $tname = $foo[0];
			my $defcount = 0;

			if (exists($h_parent_def_fixup{$tname}))
			{
				$defcount =	$h_parent_def_fixup{$tname};
			}
			else
			{
				$h_parent_def_fixup{$tname} = $defcount;
			}

			# replace all of the "**def**" entries with numbered
			# default partition names, eg "def1"
			while ($mdf_txt =~ m/\*\*def\*\*/)
			{
				$defcount++;
				my $defname = "def" . $defcount;

				$mdf_txt =~ s/\*\*def\*\*/$defname/;
			}
			
			$vv->{pby}->{makdeflt_txt} = $mdf_txt;
			$h_parent_def_fixup{$tname} = $defcount;

        }
    }

} # end genpartdefs5

# print the partition definitions

sub printpartdefs1
{
    my $reg_tabs = shift;
    my $phases = shift;

    my @foo = sort(keys(%{$reg_tabs}));

	my $phasetwo; # only set if doing EXCHANGE

    for my $ph (@{$phases})
    {
        if ($ph =~ m/create/i)
        {
            print "\n-- CREATE PARTITIONED TABLES\n\n";
        }
        elsif ($ph =~ m/mdf/i)
        {
            print "\n-- CREATE DEFAULT PARTITIONS\n\n";

        }
        elsif ($ph =~ m/xfr/i)
        {
            print "\n-- TRANSFER DATA TO PARTITIONED TABLES\n\n";

        }
        elsif ($ph =~ m/xch/i)
        {
            print "\n-- EXCHANGE DATA TO PARTITIONED TABLES\n\n";

			# need to transfer remaining data after exchange in phase two
			$phasetwo = ['XFR'];

			# cleanup if necessary
			if ($phases->[-1] =~ m/cln/i)
			{
				$phasetwo = ['XFR', 'CLN'];
			}

        }
        elsif ($ph =~ m/cln/i)
        {
			# cleanup happens in phase two (if defined)
			goto l_do_phasetwo
				if (defined($phasetwo));

            print "\n-- CLEANUP ORIGINAL TABLES\n\n";
        }

        for my $kk (@foo)
        {
            my $vv = $reg_tabs->{$kk};

#			print "include: $vv->{include}\n";

			next
				unless ($vv->{include});

            if (exists($vv->{pby}))
            {
                if ($ph =~ m/create/i)
                {
                    if (exists($vv->{pby}->{is_subpartition}))
                    {
                        print "\n-- table $kk is subpartition of $vv->{pby}->{parent}\n";
                    }
                    else
                    {
                        my $ct_txt = $vv->{pby}->{ct_txt};
                        my $pby_txt = $vv->{pby}->{pby_txt};
                        my $spec_txt = $vv->{pby}->{spec_txt};

                        print "\n";
                        print $ct_txt . "\n" . $pby_txt . "\n" . $spec_txt . ";\n";

						my $priv_txt = $vv->{pby}->{priv_txt};

						print "\n", $priv_txt, "\n"
							if (length($priv_txt));

                    }
                }
                elsif ($ph =~ m/mdf/i)
                {
                    my $mdf_txt = $vv->{pby}->{makdeflt_txt};

                    print "\n", $mdf_txt
						;
#                        unless (exists($vv->{pby}->{is_subpartition}));

                }
                elsif ($ph =~ m/xfr/i)
                {
                    my $xfr_txt = $vv->{pby}->{transfer_txt};

                    print "\n", $xfr_txt
                        unless (exists($vv->{pby}->{is_subpartition}));

                }
                elsif ($ph =~ m/xch/i)
                {
                    my $xch_txt = $vv->{pby}->{exchange_txt};

                    print "\n", $xch_txt;
                }
                elsif ($ph =~ m/cln/i)
                {
                    my $cln_txt = $vv->{pby}->{cleanup_txt};

                    if (exists($vv->{pby}->{is_subpartition}))
                    {
                        print "\n-- table $kk is subpartition of $vv->{pby}->{parent}\n";
                    }
					else
					{
						print "\n", $cln_txt;
					}
                }

            } # end if pby

        } # end for kk (@foo)
    } # end for phases

l_do_phasetwo:

	if (defined($phasetwo))
	{
		# after exchanging subpartitions, transfer remaining data from
		# parent tables, and cleanup if necessary.  Note that regular
		# transfer/cleanup phases only run on parent tables anyway, so
		# no extra work is necessary to avoid duplicate work on
		# subpartitions.

		print "\n-- Note: parent tables may still contain data after EXCHANGE phase.\n";
		print "-- transfer remaining data... \n\n";

		return printpartdefs1($reg_tabs, $phasetwo);
	}

} # end printpartdefs1

sub printpartdefs
{
    my $reg_tabs = shift;
    my $phases = shift;

    my $verzion = "unknown";
    my $now = localtime;

    if (q$Revision$ =~ /\d+/)
    {
        $verzion = do { my @r = (q$Revision$ =~ /\d+/g); sprintf "%d."."%02d" x $#r, @r }; # must be all one line, for MakeMaker
    }
        my $format_fix = << "EOF_formatfix";
                                ))}
EOF_formatfix
    # NOTE: define $format_fix with HERE document just to fix emacs
    # indenting due to comment char in Q expression...

    $verzion = $0 . " version " . $verzion . ", " . $now;
    print "-- upgrade script generated by $verzion\n";

    return printpartdefs1($reg_tabs, $phases);
} # end printpartdefs


sub classify_range_merge
{
    my ($spec1, $spec2) = @_;

    my $npartSpec = {};
    my $specstart = {};
    my $specend   = {};
    
    $npartSpec->{colh} = {};
    $npartSpec->{oper} = "RANGE";
    $npartSpec->{typ}  = "RANGE";

    $specstart->{colh} = {};
    $specstart->{colh_oper} = {};
    $specstart->{oper} = ();
    
    $specend->{colh} = {};
    $specstart->{colh_oper} = {};
    $specend->{oper} = ();

    $npartSpec->{range_start} = $specstart;
    $npartSpec->{range_end}   = $specend;

#    print "merging...\n";

    # build a copy of the range_start of spec1

    while ( my ($kk, $vv) = each(%{$spec1->{range_start}->{colh}}))
    {
        $specstart->{colh}->{$kk} = []
            unless (exists($specstart->{colh}->{$kk}));
        $specstart->{colh_oper}->{$kk} = []
            unless (exists($specstart->{colh_oper}->{$kk}));

        push (@{$specstart->{colh}->{$kk}}, @{$vv});
        push (@{$specstart->{colh_oper}->{$kk}}, 
              @{$spec1->{range_start}->{colh_oper}->{$kk}});
    }        

    # merge in the range_start from spec2

    while ( my ($kk, $vv) = each(%{$spec2->{range_start}->{colh}}))
    {
        if (exists($specstart->{colh}->{$kk}))
        {
            print STDERR "duplicate range start specification for:\n",
            Data::Dumper->Dump([$spec1]), "\n",
            Data::Dumper->Dump([$spec2]), "\n";
            return undef;
        }
        $specstart->{colh}->{$kk} = [];
        $specstart->{colh_oper}->{$kk} = [];

        push (@{$specstart->{colh}->{$kk}}, @{$vv});
        push (@{$specstart->{colh_oper}->{$kk}}, 
              @{$spec2->{range_start}->{colh_oper}->{$kk}});
    }        

    # build a copy of the range_end of spec1

    while ( my ($kk, $vv) = each(%{$spec1->{range_end}->{colh}}))
    {
        $specend->{colh}->{$kk} = []
            unless (exists($specend->{colh}->{$kk}));
        $specend->{colh_oper}->{$kk} = []
            unless (exists($specend->{colh_oper}->{$kk}));

        push (@{$specend->{colh}->{$kk}}, @{$vv});
        push (@{$specend->{colh_oper}->{$kk}}, 
              @{$spec1->{range_end}->{colh_oper}->{$kk}});
    }        

    # merge in the range_end from spec2

    while ( my ($kk, $vv) = each(%{$spec2->{range_end}->{colh}}))
    {
        if (exists($specend->{colh}->{$kk}))
        {
            print STDERR "duplicate range end specification for:\n",
            Data::Dumper->Dump([$spec1]), "\n",
            Data::Dumper->Dump([$spec2]), "\n";
            return undef;
        }
        $specend->{colh}->{$kk} = [];
        $specend->{colh_oper}->{$kk} = [];

        push (@{$specend->{colh}->{$kk}}, @{$vv});
        push (@{$specend->{colh_oper}->{$kk}}, 
              @{$spec2->{range_end}->{colh_oper}->{$kk}});
    }        


    return $npartSpec;

} # end classify_range_merge


sub classify_range_fixup
{
    my $partSpec = shift;

    my $npartSpec = {};
    my $specstart = {};
    my $specend   = {};
    
    $npartSpec->{colh} = {};
    $npartSpec->{oper} = "RANGE";
    $npartSpec->{typ}  = "RANGE";

    $specstart->{colh} = {};
    $specstart->{colh_oper} = {};
    $specstart->{oper} = ();
    
    $specend->{colh} = {};
    $specstart->{colh_oper} = {};
    $specend->{oper} = ();

    $npartSpec->{range_start} = $specstart;
    $npartSpec->{range_end}   = $specend;
    
    if ($partSpec->{typ} =~ m/RANGE_START/)
    {
        while ( my ($kk, $vv) = each(%{$partSpec->{colh}}))
        {
            $specstart->{colh}->{$kk} = []
                unless (exists($specstart->{colh}->{$kk}));
            $specstart->{colh_oper}->{$kk} = []
                unless (exists($specstart->{colh_oper}->{$kk}));
            push (@{$specstart->{colh}->{$kk}}, @{$vv});

            unless (1 == scalar(@{$vv}))
            {
                print STDERR "bad range start\n";
                return undef;
            }

            push (@{$specstart->{colh_oper}->{$kk}}, $partSpec->{oper});
        }        

    }
    elsif ($partSpec->{typ} =~ m/RANGE_END/)
    {
        while ( my ($kk, $vv) = each(%{$partSpec->{colh}}))
        {
            $specend->{colh}->{$kk} = []
                unless (exists($specend->{colh}->{$kk}));
            $specend->{colh_oper}->{$kk} = []
                unless (exists($specend->{colh_oper}->{$kk}));
            push (@{$specend->{colh}->{$kk}}, @{$vv});

            unless (1 == scalar(@{$vv}))
            {
                print STDERR "bad range end\n";
                return undef;
            }

            push (@{$specend->{colh_oper}->{$kk}}, $partSpec->{oper});
        }        

    }
    else
    {
        print STDERR "bad range spec\n",
        Data::Dumper->Dump([$partSpec]);
        return undef;
    }

    return $npartSpec;

} # end classify_range_fixup

sub classify_and
{
    my ($node, $partSpec) = @_;

    if ($node->{val} =~ m/^AND$/i)
    {
        # compound list
        unless (exists($node->{left})
                && exists($node->{left}->{partSpec})
                && exists($node->{right})
                && exists($node->{right}->{partSpec}))
        {
            return undef;
        }

        my $left  = $node->{left};
        my $right = $node->{right};

        # AND of LIST must match
        unless (exists($left->{partSpec}->{typ})
                && exists($right->{partSpec}->{typ}))
        {
            return undef;
        }

        if ($left->{partSpec}->{typ} ne
            $right->{partSpec}->{typ})
        {
            print STDERR "cannot match types for \n",
            Data::Dumper->Dump([$left->{partSpec}]),
            "\n",
            Data::Dumper->Dump([$right->{partSpec}]),
            "\n";                    
            return undef;
        }

        if ($left->{partSpec}->{typ} eq "LIST")
        {
            $partSpec->{typ} = "LIST";
            $partSpec->{oper} = "LIST";

            while ( my ($kk, $vv) = each(%{$left->{partSpec}->{colh}}))
            {
                $partSpec->{colh}->{$kk} = []
                    unless (exists($partSpec->{colh}->{$kk}));
                push (@{$partSpec->{colh}->{$kk}}, @{$vv});
            }        

            while ( my ($kk, $vv) = each(%{$right->{partSpec}->{colh}}))
            {
                $partSpec->{colh}->{$kk} = []
                    unless (exists($partSpec->{colh}->{$kk}));
                push (@{$partSpec->{colh}->{$kk}}, @{$vv});
            }        

            # XXX XXX:
            delete $left->{partSpec};
            delete $right->{partSpec};

            $node->{partSpec} = $partSpec;

        }
        elsif ($left->{partSpec}->{typ} eq "RANGE")
        {
            $partSpec = 
                classify_range_merge(
                                     $left->{partSpec},
                                     $right->{partSpec}
                                     );

            # XXX XXX:
            delete $left->{partSpec};
            delete $right->{partSpec};

            $node->{partSpec} = $partSpec;
        }
        else
        {
            # XXX XXX: fixme
            return undef;

        }


    } # end AND

    return $partSpec;
}

sub classify_or
{
    my ($node, $partSpec) = @_;

    if ($node->{val} =~ m/^OR$/i)
    {
        # compound list
        unless (exists($node->{left})
                && exists($node->{left}->{partSpec})
                && exists($node->{right})
                && exists($node->{right}->{partSpec}))
        {
            return undef;
        }

        # OR of LIST must match
        unless (exists($node->{left}->{partSpec}->{typ})
                && exists($node->{right}->{partSpec}->{typ}))
        {
            return undef;
        }

        my $left  = $node->{left};
        my $right = $node->{right};

        if ($left->{partSpec}->{typ} ne
            $right->{partSpec}->{typ})
        {
            print STDERR "cannot match types for \n",
            Data::Dumper->Dump([$left->{partSpec}]),
            "\n",
            Data::Dumper->Dump([$right->{partSpec}]),
            "\n";                    
            return undef;
        }
                
        if ($left->{partSpec}->{typ} ne "LIST")
        {
            print STDERR "cannot OR non-LIST types \n",
            Data::Dumper->Dump([$left->{partSpec}]),
            "\n",
            Data::Dumper->Dump([$right->{partSpec}]),
            "\n";                    
            return undef;
        }

        my @l1 = sort(keys(%{$left->{partSpec}->{colh}}));
        my @r1 = sort(keys(%{$right->{partSpec}->{colh}}));

        if (scalar(@l1) != scalar(@r1))
        {
            print STDERR "column number disparity for \n",
            Data::Dumper->Dump([$left->{partSpec}]),
            "\n",
            Data::Dumper->Dump([$right->{partSpec}]),
            "\n";                    
            return undef;
        }
                
        for my $col1 (@l1)
        {
            my $col2 = shift @r1;
            
            if ($col1 ne $col2)
            {
                print STDERR "column name disparity for \n",
                Data::Dumper->Dump([$left->{partSpec}]),
                "\n",
                Data::Dumper->Dump([$right->{partSpec}]),
                "\n";                    
                return undef;
            }

        } # end for

        $partSpec->{typ} = "LIST";
        $partSpec->{oper} = "LIST";

        while ( my ($kk, $vv) = each(%{$left->{partSpec}->{colh}}))
        {
            $partSpec->{colh}->{$kk} = []
                unless (exists($partSpec->{colh}->{$kk}));
            push (@{$partSpec->{colh}->{$kk}}, @{$vv});
        }        

        while ( my ($kk, $vv) = each(%{$right->{partSpec}->{colh}}))
        {
            $partSpec->{colh}->{$kk} = []
                unless (exists($partSpec->{colh}->{$kk}));
            push (@{$partSpec->{colh}->{$kk}}, @{$vv});
        }        

        # XXX XXX:
        delete $left->{partSpec};
        delete $right->{partSpec};

        $node->{partSpec} = $partSpec;

    } # end OR

    return $partSpec;
}

sub classify_hash
{
    my ($node, $partSpec) = @_;

    $partSpec->{typ} = "HASH";

    $partSpec->{hash_cols} = [];
    $partSpec->{hash_num} = -1;
    $partSpec->{hash_max} = -1;

    my @foo = 
        ($node->{where} =~ 
         #                cols      %    maxn    +   maxn    =   hash # 
         m/^\(\(hashtext\((.*)\)\s+\%\s+(\d+)\s+\+\s+(\d+).*\=\s+(\d+)/i);

    
    if (scalar(@foo) != 4)
    {
        print STDERR "bad hashtext function: ", $node->{where}, "\n";
        return undef;
    }

#    print Data::Dumper->Dump(\@foo);

    my $allcols = shift @foo;

    $partSpec->{hash_num} = pop @foo;
    $partSpec->{hash_max} = pop @foo;

    my @cols = split(/\|\|/, $allcols);

    for my $c1 (@cols)
    {
        $c1 =~ s/\:\:text//gm;
        $c1 =~ s/^\s*new\.//gm;
        $c1 =~ s/^\s+//gm;
        $c1 =~ s/\s+$//gm;

        push @{$partSpec->{hash_cols}}, $c1;
    }


    $node->{partSpec} = $partSpec;    

    return $partSpec;
} # end classify_hash

# classify_fn: for LIST and RANGE partitions
sub classify_fn
{
    my ($node, $partSpec) = @_;

    return undef
        unless ((ref($node) eq 'HASH') && exists($node->{val}));

#    print "classify_fn: ", Data::Dumper->Dump([$node]);

    if (exists($node->{left})
        && (ref($node->{left}) eq 'HASH')
        && (!exists($node->{left}->{partSpec})))
    {

        my $lpartSpec = {};

        $lpartSpec->{colh} = {};
        $lpartSpec->{oper} = ();

        $lpartSpec = classify_fn($node->{left}, $lpartSpec);
    }
    if (exists($node->{right})
        && (ref($node->{right}) eq 'HASH')
        && (!exists($node->{right}->{partSpec})))
    {

        my $lpartSpec = {};

        $lpartSpec->{colh} = {};
        $lpartSpec->{oper} = ();

        $lpartSpec = classify_fn($node->{right}, $lpartSpec);
    }
    
#		print  Data::Dumper->Dump([$node]), "\n";

    if ($node->{val} =~ m/^(\<|\>|\=|\<\>|\<\=|\>\=)$/)
    {
        # simple list

        my @foo = ($node->{val} =~ m/^(\<|\>|\=|\<\>|\<\=|\>\=)$/ );

        my $oper = $foo[0];
        
        my ($colname, $colval);

        if ($node->{left} =~ m/^new\./)
        {
            $colname = $node->{left};
            $colval  = $node->{right};
        }
        elsif ($node->{right} =~ m/^new\./)
        {
            $colname = $node->{right};
            $colval  = $node->{left};

            # normalize oper for colname on left
            if ($colname !~ m/^\<\>/)
            {
                if ($colname =~ m/^\</)
                {
                    $oper =~ s/^\</\>/;
                }
                elsif ($colname =~ m/^\>/)
                {
                    $oper =~ s/^\>/\</;
                }
            }

        }
        else
        {
            # if WHERE clause in rule does not have a column name 
            # (eg WHERE 1=1)
            print STDERR "no colname!!!\n\n";
            print STDERR Data::Dumper->Dump([$node]), "\n";
            $partSpec->{typ} = "UNKNOWN";
            return $partSpec;
        }

		# remove the "new." prefix
        $colname =~ s/^new\.//;

		# weird fixup for case like : new.bb::text = 'golf'::character varying
		$colname =~ s/\:\:\w+$//;

        $partSpec->{colh}->{$colname} = []
            unless (exists($partSpec->{colh}->{$colname}));


        push @{$partSpec->{colh}->{$colname}}, $colval;

        $partSpec->{oper} = $oper;

#		print  Data::Dumper->Dump([$node]), "\n";
#		print  Data::Dumper->Dump([$partSpec]), "\n";

        # equality comparison indicates a LIST partition, while
        # greater than/less than comparison indicates a RANGE
        # partition.
        if ($oper =~ m/^\=$/)
        {
            $partSpec->{typ} = "LIST";
        }
        elsif ($oper =~ m/^(\>|\>\=)$/ )
        {
            $partSpec->{typ} = "RANGE_START";
        }
        elsif ($oper =~ m/^(\<|\<\=)$/ )
        {
            $partSpec->{typ} = "RANGE_END";
        }
        else
        {
            $partSpec->{typ} = "UNKNOWN";
        }

        $partSpec = classify_range_fixup($partSpec)
            if ($partSpec->{typ} =~ m/^RANGE/);

        $node->{partSpec} = $partSpec;
    }


    if ($node->{val} =~ m/^OR$/i)
    {
        $partSpec = classify_or($node, $partSpec);
    }

    if ($node->{val} =~ m/^AND$/i)
    {
        $partSpec = classify_and($node, $partSpec);
    }


    return $partSpec;
}

# classify: the top-level classifier.  Parse the rewrite rules and
# determine if a RANGE or LIST partition.
sub classify
{
    my $node = shift;

    return 0
        unless (exists($node->{parsetree}));

    my $partSpec = {};

    $partSpec->{colh} = {};
    $partSpec->{oper} = ();

    if ($node->{where} =~ m/^\(\(hashtext\(.*\)\s+\%\s+/i)
    {
        $partSpec = classify_hash($node, $partSpec);
        $node->{parsetree}->{partSpec} = $partSpec;
    }
    else
    {
        $partSpec = classify_fn($node->{parsetree}, $partSpec);
    }
#    print Data::Dumper->Dump([$partSpec]);
}

sub prettyprintPartSpec
{
    my $partSpec = shift;

    if ($partSpec->{typ} eq "HASH")
    {
        print "  Columns: (", 
        join(", ", @{$partSpec->{hash_cols}}), ")\n";
        print "  Partition Number ", $partSpec->{hash_num},
        " of ", $partSpec->{hash_max}, "\n";
    }
    elsif ($partSpec->{typ} eq "LIST")
    {
        print "  Columns: (", 
        join(", ", sort(keys(%{$partSpec->{colh}}))), ")\n";

        my @foo = sort(keys(%{$partSpec->{colh}}));
        my $maxn = scalar(@{$partSpec->{colh}->{$foo[0]}});

        my @vals;

        for my $ii (0..($maxn-1))
        {
            for my $coln (@foo)
            {
                push @vals, $partSpec->{colh}->{$coln}->[$ii];
            }
        }
        print "  Values: (", 
        join(", ", @vals), ")\n";

    }
    elsif ($partSpec->{typ} eq "RANGE")
    {
        my @rt = qw(range_start range_end);

        for my $rtv (@rt)
        {
            if ($rtv =~ m/start/)
            {
                print "  Start: \n";
            }
            else
            {
                print "  End: \n";
            }
        
            my $pspec = $partSpec->{$rtv};
            my @foo = sort(keys(%{$pspec->{colh}}));
            my $maxn = scalar(@{$pspec->{colh}->{$foo[0]}});

            for my $cnam (@foo)
            {
                print "   ", $cnam, " ", $pspec->{colh_oper}->{$cnam}->[0],
                " ",
                $pspec->{colh}->{$cnam}->[0], "\n";
            }

        }
        

    }
}

sub prettydump
{
    my $reg_tabs = shift;

    my @tabnames = sort(keys(%{$reg_tabs}));

    for my $kk (@tabnames)
    {
        my $vv = $reg_tabs->{$kk};

        next 
            unless (exists($vv->{rules}));

        print "Table: $kk\n";

        if (exists($vv->{rules}))
        {
            my @r1 = sort(keys(%{$vv->{rules}}));

            for my $kk2 (@r1)
            {
                my $vv2 = $vv->{rules}->{$kk2};

                next
                    unless (exists($vv2->{parsetree}));

				unless (ref($vv2->{parsetree}) eq "HASH")
				{
					print "weirdo: $kk\n";
					print Data::Dumper->Dump([$vv2]),"\n";
					print "skipping: $kk\n";
					next;
				}

				next unless (exists($vv2->{parsetree}->{partSpec}));

                print "  Child: ", $vv2->{child}, "\n";

                my $partSpec = $vv2->{parsetree}->{partSpec};
                
                print "  Partition Type: ", $partSpec->{typ}, "\n";
                prettyprintPartSpec($partSpec);                
                            
            }
        }

    }
} # end prettydump

sub get_owner_privs
{
	my $tname = shift;

    my $psql_str = "psql ";

    $psql_str .= $glob_connect
        if (defined($glob_connect));

	# describe table and get owner
	$psql_str .= " -c \'\\dt  $tname\'";

#    print $psql_str, "\n";

    my $tabdef = `$psql_str`;

	# remove pre-header -- need table header
	$tabdef =~ s/^\s+List of relations\s*//;

#	print $tabdef, "\n";

    my $rowarr = tablelizer($tabdef);

#	print Data::Dumper->Dump([$rowarr]);

	my $owner;

	if (scalar(@{$rowarr}))
	{
		my $r1 = pop @{$rowarr};
		$owner = $r1->{Owner};
	}

	my $outi = {};
	
	$outi->{owner} = $owner;

	$psql_str = "psql ";

	$psql_str .= $glob_connect
		if (defined($glob_connect));

	# describe table and get privileges
	$psql_str .= " -c \'\\dp  $tname\'";

	$tabdef = `$psql_str`;

	# remove pre-header -- need table header
	my @ttt = split(/\n/, $tabdef);

	shift @ttt; # drop "Access privileges for database..."

	$tabdef = join("\n", @ttt);

	$rowarr = tablelizer($tabdef);

	my $acl;

	if (scalar(@{$rowarr}))
	{
		my $r1 = pop @{$rowarr};
		$acl = $r1->{"Access privileges"};
	}

	if ($acl)
	{
		$outi->{acl} = $acl;

		$acl =~ s/^\{//;
		$acl =~ s/\}$//;

		my @privs = split(/\,/, $acl);

		my $grant_regular = {};
		my $grant_with_grant = {};

		for my $pr1 (@privs)
		{
			# remove any quotes
			$pr1 =~ s/^\"//;
			$pr1 =~ s/\"$//;

			# remove the grantor
			my @p2 = split(/\//, $pr1);

			my $p3 = shift @p2; # first part is name/role/group = priv list

			$p3 = "PUBLIC" . $p3
				if ($p3 =~ m/^\=/); # change anonymous role to PUBLIC

			my @name_list = split(/\=/, $p3);

			my $grant_name = $name_list[0];

			if ($name_list[1] =~ m/^arwdRxt$/)
			{
				$grant_regular->{$grant_name} = ["ALL PRIVILEGES"];
			}
			else
			{
				my @alphalist = split(//, $name_list[1]);

				$grant_with_grant->{$grant_name} = []
					unless (exists($grant_with_grant->{$grant_name}));

				$grant_regular->{$grant_name} = []
					unless (exists($grant_regular->{$grant_name}));

				for my $a1 (@alphalist)
				{
					my $tt;

					$tt = undef;

					if ($a1 =~ m/\*/)
					{
						my $last_priv = pop @{$grant_regular->{$grant_name}};

						push @{$grant_with_grant->{$grant_name}}, $last_priv;
						
					}
					else
					{
						if ($a1 =~ m/r/)
						{
							$tt = "SELECT";
						}
						elsif ($a1 =~ m/w/)
						{
							$tt = "UPDATE";
						}
						elsif ($a1 =~ m/a/)
						{
							$tt = "INSERT";
						}
						elsif ($a1 =~ m/d/)
						{
							$tt = "DELETE";
						}
						elsif ($a1 =~ m/R/)
						{
							$tt = "RULE";
						}
						elsif ($a1 =~ m/x/)
						{
							$tt = "REFERENCES";
						}
						elsif ($a1 =~ m/t/)
						{
							$tt = "TRIGGER";
						}
						elsif ($a1 =~ m/X/)
						{
							$tt = "EXECUTE";
						}
						elsif ($a1 =~ m/U/)
						{
							$tt = "USAGE";
						}
						elsif ($a1 =~ m/C/)
						{
							$tt = "CREATE";
						}
						elsif ($a1 =~ m/T/)
						{
							$tt = "TEMPORARY";
						}
						else
						{
							# complain
						}
						push @{$grant_regular->{$grant_name}}, $tt;

					}
				

				} # end for alphalist

			} # end if not all privs


		} # end for all @privs
		
		$outi->{grant_with_grant} = $grant_with_grant;
		$outi->{grant_regular} = $grant_regular;


	} # end if acl

#	print Data::Dumper->Dump([$outi]);

	return $outi;
} # end get_owner_privs

if (1)
{
    my $psql_str = "psql ";

    $psql_str .= $glob_connect
        if (defined($glob_connect));

    $psql_str .= " -c \'select * from pg_rules \' ";

#    print $psql_str, "\n";

    my $tabdef = `$psql_str`;


    my $rowarr = tablelizer($tabdef);

	# build a hash by schema.tablename of all the pg_rules
	# information.  Later, parse out specific partitioning rules,
	# parent/child relationships, etc.
    my %reg_tabs;

    for my $rr (@{$rowarr})
    {
        if ($rr->{definition} =~ m/AS ON INSERT TO/i)
        {

			# NOTE: assume no duplicate INSERT rewrite rules on same table
            $reg_tabs{ $rr->{schemaname} . "." . $rr->{tablename} } = $rr;

            $rr->{Schema} = $rr->{schemaname};
        }

    }

#	print Data::Dumper->Dump($rowarr),"\n";

	# MPP-6488: need to DROP CONSTRAINTS for EXCHANGE.
	# build hash of all constraints by schema.tablename.  Each element
	# contains an array of constraint names.
	my %all_cons; 

	if (1)
	{
		$psql_str = "psql ";

		$psql_str .= $glob_connect
			if (defined($glob_connect));

		$psql_str .= " -c \'select pcon.conname, pc.relname, " .
			" pn.nspname, pcon.consrc " . 
			"from pg_constraint pcon, pg_class pc, pg_namespace pn " .
			"where pcon.conrelid = pc.oid and pc.relnamespace = pn.oid " .
			"order by pcon.conname, pn.nspname, pc.relname, pcon.consrc  \' ";

#    print $psql_str, "\n";

		$tabdef = `$psql_str`;

		$rowarr = tablelizer($tabdef);

#	print Data::Dumper->Dump($rowarr),"\n";

		for my $rr (@{$rowarr})
		{
			my $tname = $rr->{nspname} . "." . $rr->{relname};

			my $con_item = {name => $rr->{conname}, src => $rr->{consrc}};

			if (exists($all_cons{$tname}))
			{
				push @{$all_cons{$tname}}, $con_item;
			}
			else
			{
				$all_cons{$tname} = [$con_item];
			}

		}
	} # end get all_cons

	# for each table in pg_rules, describe it and get the rewrite
	# rule, the WHERE clause for the partition rule/check constraint,
	# and the child table (the target of the INSERT)
    while (my ($kk, $vv) = each (%reg_tabs))
    {
        $psql_str = "psql ";

        $psql_str .= $glob_connect
            if (defined($glob_connect));

        $psql_str .= " -c \'\\d  $kk\'";
        
        $tabdef = `$psql_str`;

        $vv->{describe} = $tabdef;

        next
            unless ($tabdef =~ m/^Rules\:/m);

		my $owner_privs = get_owner_privs($kk);

		$vv->{owner_privs} = $owner_privs;

#        print $tabdef;


        # look for "Rules: " in column zero, and use it to pattern
        # match remainder of describe output...
        my @ruletxt = ($tabdef =~ m/\nRules\:(.*)/s);

#        print $ruletxt[0], "\n";

        next
            unless (scalar(@ruletxt));

        my $r1 = $ruletxt[0];

        # look for next "<tag>: " in column zero, and use it to
        # delimit set of all rules...
        @ruletxt = split(/\n\w+((\s+\w+)*)\:/s, $r1, 2);

        next
            unless (scalar(@ruletxt));

        $r1 = $ruletxt[0];

        @ruletxt = split(/\n/, $r1);

#        print Data::Dumper->Dump(\@ruletxt);

        my $rules = {};
        my $rulenam;
        my $ruleitm;

        for my $lin (@ruletxt)
        {
            next
                unless (length($lin));

            if ($lin =~ m/AS\s*$/)
            {
                if (defined($rulenam))
                {
                    $rules->{$rulenam} = $ruleitm;
                    $rulenam = undef;
                }

                my @foo = ($lin =~ m/\s+(.*)\s+AS\s*$/);

                unless (scalar(@foo))
                {
                    print STDERR "bad line1 : $lin\n";
                    exit(1);
                }

                $rulenam = $foo[0];
                $ruleitm = {};
				
				$ruleitm->{ruledef} = $lin;
				$ruleitm->{ruledef} =~ s/^\s+//;

            }
			else
			{
				$ruleitm->{ruledef} .= "\n" . $lin;
			}

            unless (defined($rulenam))
            {
                print STDERR "no rulename\n";
                exit(1);
            }
            
            if ($lin =~ m/^\s+ON INSERT TO\s*.*$/)
            {
                my @foo = ($lin =~ m/^\s+ON INSERT TO\s+(.*)\s*$/);
                
                unless (scalar(@foo))
                {
                    print STDERR "bad line2 : $lin\n";
                    exit(1);
                }
                
                $ruleitm->{parent} = $foo[0];
            }

            if ($lin =~ m/^\s+WHERE.*DO INSTEAD/)
            {
                my @foo = 
       ($lin =~ m/^\s+WHERE\s+(.*)\s+DO INSTEAD\s+INSERT\s+INTO\s+(.*)\s+\(.*$/);

                unless (scalar(@foo) > 0)
                {
                    print STDERR "bad line3 : $lin\n";
                    exit(1);
                }

                $ruleitm->{where} = $foo[0];
                $ruleitm->{child} = $foo[1];
            }


        } # end for ruletxt

        if (defined($rulenam))
        {
            $rules->{$rulenam} = $ruleitm;
            $rulenam = undef;
        }

        $vv->{rules} = $rules;

#        print Data::Dumper->Dump([$vv]), "\n";

    } # end while each reg_tabs


	# determine RANGE vs LIST tables
    while (my ($kk, $vv) = each (%reg_tabs))
    {
        if (exists($vv->{rules}))
        {
            my $rr = $vv->{rules};
            while (my ($kk2, $vv2) = each (%{$rr}))
            {
#                print STDERR $kk2, ":\n";
                if (exists($vv2->{where}))
                {
                    my ($extracted, $remainder, $txt);

                    $txt = $vv2->{where};
#                    print "  where: ", $txt, "\n";

                    # parse words
					$txt = typename_fixup_in($txt);

                    my $biga = pw1($txt);

                    $vv2->{parsetree} = $biga;

                    # classify the tables
                    classify($vv2);
#                    print Data::Dumper->Dump($biga);
                }
            }

        }

    }

    if (0)
    {
        while (my ($kk, $vv) = each (%reg_tabs))
        {
            if (exists($vv->{rules}))
            {
                my $desc = $vv->{describe};
                delete $vv->{describe};
                print Data::Dumper->Dump([$vv]), "\n";

                $vv->{describe} = $desc;
            }
        }
    }


#    print Data::Dumper->Dump([%reg_tabs]), "\n";

    # generate partition definitions

	# 1: generate the specs and sort the range partitions
    genpartdefs(\%reg_tabs);
	# 2: fixup invalid partition specs, ie "point" ranges where start==end
    genpartdefs2(\%reg_tabs);
	# 3: add subpartitions to the partition by clause
    genpartdefs3(\%reg_tabs);
	# 4: build text and comments for partition by clause
    genpartdefs4(\%reg_tabs, \%all_cons);
	# 5: hang the subpartition specs in the right places, 
	# and fixup default partition names
    genpartdefs5(\%reg_tabs);
    
#    print Data::Dumper->Dump([%reg_tabs]), "\n";

    print "\n\n\n";

    if ($glob_dosql)
    {
        printpartdefs(\%reg_tabs, $glob_dosql);
    }
    else
    {
        prettydump(\%reg_tabs);
    }

    print "\n\n\n";



}


exit();
