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
my $pg_operator_key = "pg_operator";
my $pg_opclass_key = "pg_opclass";

my %catalog_data;
my $output_default_data    = [];
my $output_cross_data    = [];

foreach my $input_file (@ARGV)
{
	$input_file =~ /(\w+)\.dat$/
		or die "Input files need to be .dat files.\n";
	my $catname = $1;
	my %data = ParseData($input_file);
	say $catname;
	$catalog_data{$catname} = \%data;
}

my $amproc_ref = $catalog_data{$pg_amproc_key};
my $amop_ref = $catalog_data{$pg_amop_key};
my $proc_ref = $catalog_data{$pg_proc_key};
my $operator_ref = $catalog_data{$pg_operator_key};
my $opclass_ref = $catalog_data{$pg_opclass_key};
my $create_sql;
my $alter_sql;
my $create_bt_sql;
my $alter_bt_sql;
#print ref \%catalog_data, "\n";
#print ref $amproc_ref, "\n";
#print ref $amop_ref, "\n";
#print ref $proc_ref, "\n";
my %default_type;

my %btreemap;
foreach my $key (keys %$amop_ref)
{
    my @opfamily_split = split(/\//, $key);
    if ($opfamily_split[0] eq "btree") {
        my %opfamily_all_ops = %{$amop_ref->{$key}};
        foreach my $type_key (keys %opfamily_all_ops)
        {
            my @opfamily_ops = @{$opfamily_all_ops{$type_key}};
            foreach my $op_ref ( @opfamily_ops )
            {
                if ($op_ref->{amoplefttype} eq $op_ref->{amoprighttype})
                {
                    $btreemap{$op_ref->{amoplefttype}} = $opfamily_split[1];
                }
            }
        }
    }
}

foreach my $key (keys %$amproc_ref)
{
    my @opfamily_split = split(/\//, $key);
    if ($opfamily_split[0] eq "btree") {
        my %opfamily_all_procs = %{$amproc_ref->{$key}};
        foreach my $type_key (keys %opfamily_all_procs)
        {
            my @opfamily_procs = @{$opfamily_all_procs{$type_key}};
            foreach my $opproc_ref ( @opfamily_procs )
            {
                if ($opproc_ref->{amproclefttype} eq $opproc_ref->{amprocrighttype})
                {
                    $btreemap{$opproc_ref->{amproclefttype}} = $opfamily_split[1];
                }
            }
        }
    }
}

my %pg_amop_unique_exist;

foreach my $key (keys %$amop_ref)
{
	my @opfamily_split = split(/\//, $key);
	my $sql_str_create = "CREATE OPERATOR FAMILY " . $opfamily_split[1] . " USING us" . $opfamily_split[0] . ";\n";
	my $sql_str_alter = "ALTER OPERATOR FAMILY " . $opfamily_split[1] . " USING us" . $opfamily_split[0] . " ADD\n";
	my %opfamily_all_procs = %{$amproc_ref->{$key}};
	my %opfamily_all_ops = %{$amop_ref->{$key}};
	my %opfamily_all_opclass = %{$opclass_ref->{$key}};
	my $isalter = 0;
	my %isfind;
    my $storage="";
	foreach my $opclass_key (keys %opfamily_all_opclass)
	{
        my %opclasshw_name = %{$opfamily_all_opclass{$opclass_key}[0]};
		my $type_key = $opclasshw_name{opcintype};
		my @opfamily_ops = @{$opfamily_all_ops{$type_key}};
		my $isinit = 0;
		my $pg_amop_unique_exist_key = $key;
		$pg_amop_unique_exist_key .= "_" . $opclasshw_name{opcintype};
		if (!exists($pg_amop_unique_exist{$pg_amop_unique_exist_key})) {
			$pg_amop_unique_exist{$pg_amop_unique_exist_key} = 1;
		} else {
			say $pg_amop_unique_exist_key . "_" . $opclasshw_name{opcname};
		    next;
		}
		foreach my $op_ref ( @opfamily_ops )
		{
			if ($op_ref->{amoplefttype} eq $op_ref->{amoprighttype})
			{
				if ($isinit == 0)
				{
#my %opclasshw_name = %{$opclass_ref->{$key}{$op_ref->{amoplefttype}}[0]};
                    if (exists $opclasshw_name{opckeytype}) {
                        $storage=$opclasshw_name{opckeytype};
                    }
                    $sql_str_create =~ s/,\n$/;\n/;
					$sql_str_create .= "CREATE OPERATOR CLASS " . $opclasshw_name{opcname} . "\n";
                    my $temp_key = "dt_" . $opfamily_split[0] . "_" . $op_ref->{amoplefttype};
                    if (!exists($opclasshw_name{opcdefault}))
                    {
                        $sql_str_create .= "DEFAULT ";
                        $default_type{$temp_key} = 1;
                    }
                    $sql_str_create .= "FOR TYPE ";
                    if ($op_ref->{amoplefttype} eq "char") {
                        $sql_str_create .= "\"char\"";
                    } else {
                        $sql_str_create .= $op_ref->{amoplefttype};
                    }
                    $sql_str_create .= " USING us" . $opfamily_split[0] . " FAMILY " . $opfamily_split[1] . " AS\n";
                    $isinit = 1;
				}
                my $opppr = $op_ref->{amopopr};
                $opppr =~ s/(\b)char(\b)/\"char\"/g;
				$sql_str_create .= "OPERATOR " . $op_ref->{amopstrategy} . " " . $opppr;
                my $r_operator_ref = $operator_ref->{$op_ref->{amopopr}};
                if ($r_operator_ref->{oprresult} ne "bool") {
                    my $return_type_family = $btreemap{$r_operator_ref->{oprresult}};
                    $sql_str_create .= " FOR ORDER BY " . $return_type_family;
                }
                $sql_str_create .= ",\n";
            }
			else
			{
				$isalter = 1;
                my $opppr = $op_ref->{amopopr};
                $opppr =~ s/(\b)char(\b)/\"char\"/g;
				$sql_str_alter .= "OPERATOR " . $op_ref->{amopstrategy} . " " . $opppr;
                my $r_operator_ref = $operator_ref->{$op_ref->{amopopr}};
                if ($r_operator_ref->{oprresult} ne "bool") {
                    my $return_type_family = $btreemap{$r_operator_ref->{oprresult}};
                    $sql_str_alter .= " FOR ORDER BY " . $return_type_family;
                }
                $sql_str_alter .= ",\n";
			}
		}

		if (exists($opfamily_all_procs{$type_key}))
		{
			my @opfamily_procs = @{$opfamily_all_procs{$type_key}};
			foreach my $opproc_ref ( @opfamily_procs )
			{
				my $r_proc_ref = $proc_ref->{$opproc_ref->{amproc}};
				my $fuc;
				if (!exists($proc_ref->{$opproc_ref->{amproc}}))
				{
#print ref $proc_ref, "\n";
					$fuc = $opproc_ref->{amproc};
				}
				else {
                    $r_proc_ref->{proargtypes} =~ s/ /,/g;
#my @arg_types = split(/,/, $r_proc_ref->{proargtypes});
# if (@arg_types == 2 && $arg_types[0] ne "internal" && $arg_types[0] ne "internal") {
# $fuc = $opproc_ref->{amproc} . "(" . $r_proc_ref->{amproclefttype} . "," . $r_proc_ref->{amprocrighttype} . ")";
# } else {
# $fuc = $opproc_ref-> { amproc } . "(" . $r_proc_ref->{proargtypes} . ")";
# }
                    $fuc = $opproc_ref-> { amproc } . "(" . $r_proc_ref->{proargtypes} . ")";
				}
                $fuc =~ s/(\b)char(\b)/\"char\"/g;
#$fuc =~ /\(.*\)/;
#my $fun_num = $&;
#if ($opproc_ref->{amproc} eq "in_range")
#if (rindex($opproc_ref->{amproc}, "in_range", 0) >= 0)
#{
#my $fun_length = length($fun_num);
#my $sub_fun_num = substr($fun_num, 1, $fun_length - 2);
#print $sub_fun_num,"\n";
#my @fun_vars = split(/,/, $sub_fun_num);
#my $first_var = '';
#my $second_var = '';
#foreach my $fun_var ( @fun_vars ) {
#if ($first_var eq '') {
#$first_var = $fun_var;
#} elsif (($first_var ne $fun_var) && ($fun_var ne "bool")){
#$second_var = $fun_var;
#last;
#}
#}
#$fun_num = "(" . $first_var . "," . $second_var . ")";
#}
#my $fun_num = "(" . $opproc_ref->{amproclefttype} . "," . $opproc_ref->{amprocrighttype} . ")";
                my $fun_num = "(";
                if ($opproc_ref->{amproclefttype} eq "char") {
                    $fun_num .= "\"" . $opproc_ref->{amproclefttype} . "\",";
                } else {
                    $fun_num .= $opproc_ref->{amproclefttype} . ",";
                }
                if ($opproc_ref->{amprocrighttype} eq "char") {
                    $fun_num .= "\"" . $opproc_ref->{amprocrighttype} . "\")";
                } else {
                    $fun_num .= $opproc_ref->{amprocrighttype} . ")";
                }
				if ($opproc_ref->{amproclefttype} eq $opproc_ref->{amprocrighttype})
				{
                    if ($isinit == 0)
                    {
# my %opclasshw_name = %{$opclass_ref->{$key}{$opproc_ref->{amproclefttype}}[0]};
                        if (exists $opclasshw_name{opckeytype}) {
                            $storage=$opclasshw_name{opckeytype};
                        }
                        $sql_str_create =~ s/,\n$/;\n/;
#                        $sql_str_create .= "CREATE OPERATOR CLASS " . $opclasshw_name{opcname} . "\nDEFAULT FOR TYPE " . $opproc_ref->{amproclefttype} . " USING us" . $opfamily_split[0] . " FAMILY " . $opfamily_split[1] . " AS\n";
                        $sql_str_create .= "CREATE OPERATOR CLASS " . $opclasshw_name{opcname} . "\n";
                        my $temp_key = "dt_" . $opfamily_split[0] . "_" . $opproc_ref->{amproclefttype};
                        if (!exists($opclasshw_name{opcdefault}))
# && $opproc_ref->{amproclefttype} ne "bpchar"
                        {
                            $sql_str_create .= "DEFAULT ";
                            $default_type{$temp_key} = 1;
                        }
                        $sql_str_create .= "FOR TYPE ";
                        if ($opproc_ref->{amproclefttype} eq "char") {
                            $sql_str_create .= "\"char\"";
                        } else {
                            $sql_str_create .= $opproc_ref->{amproclefttype};
                        }
                        $sql_str_create .= " USING us" . $opfamily_split[0] . " FAMILY " . $opfamily_split[1] . " AS\n";
                        $isinit = 1;
                    }
					$sql_str_create .= "FUNCTION " . $opproc_ref->{amprocnum} . $fun_num . " " . $fuc . ",\n";
				}
				else
				{
					$isalter = 1;
					$sql_str_alter .= "FUNCTION " . $opproc_ref->{amprocnum} . $fun_num . " " . $fuc . ",\n";
				}
			}
			$isfind{$type_key} = 1;
		};
	}
	foreach my $type_key (keys %opfamily_all_procs)
	{
		if (!exists($opfamily_all_procs{$type_key}))
		{
            my $isinit = 0;
			my @opfamily_procs = @{$opfamily_all_procs{$type_key}};
			foreach my $opproc_ref ( @opfamily_procs )
			{
				my $r_proc_ref = $proc_ref->{$opproc_ref->{amproc}};
				my $fuc;
				if (!exists($proc_ref->{$opproc_ref->{amproc}}))
				{
#print ref $proc_ref, "\n";
					$fuc = $opproc_ref->{amproc};
				}
				else {
                    $r_proc_ref->{proargtypes} =~ s/ /,/g;
#if (@arg_types == 2 && $arg_types[0] ne "internal" && $arg_types[0] ne "internal") {
#$fuc = $opproc_ref->{amproc} . "(" . $r_proc_ref->{amproclefttype} . "," . $r_proc_ref->{amprocrighttype} . ")";
#} else {
#$fuc = $opproc_ref->{amproc} . "(" . $r_proc_ref->{proargtypes} . ")";
#}
                    $fuc = $opproc_ref->{amproc} . "(" . $r_proc_ref->{proargtypes} . ")";
				}
                $fuc =~ s/(\b)char(\b)/\"char\"/g;
#$fuc =~ /\(.*\)/;
#my $fun_num = $&;
#if ($opproc_ref->{amproc} eq "in_range")
#if (rindex($opproc_ref->{amproc}, "in_range", 0) >= 0)
#{
#my $fun_length = length($fun_num);
#my $sub_fun_num = substr($fun_num, 1, $fun_length - 2);
#my @fun_vars = split(/,/, $sub_fun_num);
#my $first_var = '';
#my $second_var = '';
#foreach my $fun_var ( @fun_vars ) {
#if ($first_var eq '') {
#$first_var = $fun_var;
#} elsif (($first_var ne $fun_var) && ($fun_var ne "bool")){
#$second_var = $fun_var;
#last;
#}
#}
#$fun_num = "(" . $first_var . "," . $second_var . ")";
#}
                my $fun_num = "(";
                if ($opproc_ref->{amproclefttype} eq "char") {
                    $fun_num .= "\"" . $opproc_ref->{amproclefttype} . "\",";
                } else {
                    $fun_num .= $opproc_ref->{amproclefttype} . ",";
                }
                if ($opproc_ref->{amprocrighttype} eq "char") {
                    $fun_num .= "\"" . $opproc_ref->{amprocrighttype} . "\")";
                } else {
                    $fun_num .= $opproc_ref->{amprocrighttype} . ")";
                }
				if ($opproc_ref->{amproclefttype} eq $opproc_ref->{amprocrighttype})
				{
                    if ($isinit == 0)
                    {
                        my %opclasshw_name = %{$opclass_ref->{$key}{$opproc_ref->{amproclefttype}}[0]};
                        $sql_str_create =~ s/,\n$/;\n/;
                        if (exists $opclasshw_name{opckeytype}) {
                            $storage=$opclasshw_name{opckeytype};
                        }
#                        $sql_str_create .= "CREATE OPERATOR CLASS " . $opclasshw_name{opcname} . "\nDEFAULT FOR TYPE " . $opproc_ref->{amproclefttype} . " USING us" . $opfamily_split[0] . " FAMILY " . $opfamily_split[1] . " AS\n";
                        $sql_str_create .= "CREATE OPERATOR CLASS " . $opclasshw_name{opcname} . "\n";
                        my $temp_key = "dt_" . $opfamily_split[0] . "_" . $opproc_ref->{amproclefttype};
                        if (!exists($opclasshw_name{opcdefault}))
                        {
                            $sql_str_create .= "DEFAULT ";
                            $default_type{$temp_key} = 1;
                        }
# $sql_str_create .= "FOR TYPE " . $opproc_ref->{amproclefttype} . " USING us" . $opfamily_split[0] . " FAMILY " . $opfamily_split[1] . " AS\n";
                        $sql_str_create .= "FOR TYPE ";
                        if ($opproc_ref->{amproclefttype} eq "char") {
                            $sql_str_create .= "\"char\"";
                        } else {
                            $sql_str_create .= $opproc_ref->{amproclefttype};
                        }
                        $sql_str_create .= " USING us" . $opfamily_split[0] . " FAMILY " . $opfamily_split[1] . " AS\n";
                        $isinit = 1;
                    }
					$sql_str_create .= "FUNCTION " . $opproc_ref->{amprocnum} . $fun_num . " " . $fuc . ",\n";
				}
				else
				{
					$isalter = 1;
					$sql_str_alter .= "FUNCTION " . $opproc_ref->{amprocnum} . $fun_num . " " . $fuc . ",\n";
				}
			}
		}
	}
    if (length($storage) != 0 && ($opfamily_split[0] eq "brin" || $opfamily_split[0] eq "gin" || $opfamily_split[0] eq "gist" || $opfamily_split[0] eq "spgist")) {
        $sql_str_create .= "STORAGE ";
		if ($storage eq "char") {
			$sql_str_create .= "\"char\"";
		} else {
			$sql_str_create .= $storage;
		}
		$sql_str_create .= ",\n";
    }
    $sql_str_create =~ s/,\n*$//;
	$sql_str_create .= ";\n";
    $sql_str_alter =~ s/,\n*$//;
	$sql_str_alter .= ";\n";
	$create_sql .= $sql_str_create;
	if ($isalter == 1) {
		$alter_sql .= $sql_str_alter;
	}
    if ($opfamily_split[0] eq "btree") {
        $sql_str_create =~ s/usbtree/usbitmap/g;
        $sql_str_alter =~ s/usbtree/usbitmap/g;
        $create_sql .= $sql_str_create;
        if ($isalter == 1) {
            $alter_sql .= $sql_str_alter;
        }
    }
}

my $output_create_file_full_path = "create_index_ops.sql";
my $output_alter_file_full_path = "alter_index_ops.sql";
open my $create_sql_handler, '>', $output_create_file_full_path
	or die "can't open $output_create_file_full_path: $!";
open my $alter_sql_handler, '>', $output_alter_file_full_path
	or die "can't open $output_create_file_full_path: $!";

print $create_sql_handler $create_sql;
print $alter_sql_handler $alter_sql;
close $create_sql_handler;
close $alter_sql_handler;

# Parses a file containing Perl data structure literals, returning live data.
#
# The parameter $preserve_formatting needs to be set for callers that want
# to work with non-data lines in the data files, such as comments and blank
# lines. If a caller just wants to consume the data, leave it unset.
sub ParseData
{
	my ($input_file) = @_;

	open(my $ifd, '<', $input_file) || die "$input_file: $!";
	$input_file =~ /(\w+)\.dat$/
	  or die "Input file $input_file needs to be a .dat file.\n";
	my $catname = $1;
	my %data;

	# Scan the input file.
	my $istest = 0;
	while (<$ifd>)
	{
		my $hash_ref;

		if (/{/)
		{
			# Capture the hash ref
			# NB: Assumes that the next hash ref can't start on the
			# same line where the present one ended.
			# Not foolproof, but we shouldn't need a full parser,
			# since we expect relatively well-behaved input.

			# Quick hack to detect when we have a full hash ref to
			# parse. We can't just use a regex because of values in
			# pg_aggregate and pg_proc like '{0,0}'.  This will need
			# work if we ever need to allow unbalanced braces within
			# a field value.
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

				if ($catname eq $pg_amproc_key)
				{
					push @{ $data{$hash_ref->{amprocfamily}}{$hash_ref->{amproclefttype}} }, $hash_ref;
				}
				elsif ($catname eq $pg_amop_key)
				{
					push @{ $data{$hash_ref->{amopfamily}}{$hash_ref->{amoplefttype}} }, $hash_ref;

				}
				elsif ($catname eq $pg_proc_key)
				{
					if (!exists($data{$hash_ref->{proname}}))
					{
						$data{$hash_ref->{proname}} = $hash_ref;
					}
				}
                elsif ($catname eq $pg_operator_key)
                {
                    my $t_key = $hash_ref->{oprname} . "(";
                    if ($hash_ref->{oprleft} ne "0")
                    {
                        $t_key .= $hash_ref->{oprleft} . ",";
                    }
                    $t_key .= $hash_ref->{oprright} . ")";
                    $data{$t_key} = $hash_ref;
                }
                elsif ($catname eq $pg_opclass_key)
                {
                    push @{ $data{$hash_ref->{opcfamily}}{$hash_ref->{opcname}} }, $hash_ref;
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

	return %data;
}

