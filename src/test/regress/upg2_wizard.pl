#!/usr/bin/env perl
#
# $Header$
#
# copyright (c) 2009, 2010
# Author: Jeffrey I Cohen
#
#
use Pod::Usage;
use Getopt::Long;
use Data::Dumper;
use strict;
use warnings;

=head1 NAME

B<upg2_wizard.pl> - wizard to fix upg2 test

=head1 SYNOPSIS

B<upg2_wizard.pl> [options] 

Options:

    -help            brief help message
    -man             full documentation
    -connect         psql connect parameters
    -tablename       LIKE expression describing new tables
    -indexname       LIKE expression describing new indexes
    -preview         show data file updates, but don't modify the files

=head1 OPTIONS

=over 8

=item B<-help>

    Print a brief help message and exits.

=item B<-man>
    
    Prints the manual page and exits.

=item B<-connect>
    
    psql connect string, e.g:

    -connect '-p 11000 template1'

=item B<-tablename>
    
    LIKE expression describing new catalog table, eg:
	
	-tablename foo

	or

	-tablename foo%bar

	To specify multiple tables, use multiple tablename arguments, eg:

	-tablename foo -tablename bar -tablename baz

=item B<-indexname>
    
    LIKE expression describing new catalog indices, eg:
	
	-indexname foo

	or

	-indexname foo%bar

	To specify multiple indices, use multiple indexname arguments, eg:

	-indexname foo -indexname bar -indexname baz

=item B<-preview>

    Show data file updates, but don't modify the files.

=back

=head1 DESCRIPTION

The upg2_wizard is used to modify the data files for the upg2
regression test in accordance with the UPG2_README guidelines.

=head1 AUTHORS

Jeffrey I Cohen

Copyright (c) 2009, 2010 GreenPlum.  All rights reserved.  

Address bug reports and comments to: jcohen@greenplum.com


=cut

my $glob_id = "";
my $glob_connect;
my $glob_tname;
my $glob_iname;
my $glob_preview;

BEGIN {
    my $man  = 0;
    my $help = 0;
    my $conn;
    my @tname;
    my @iname;
	my $preview = 0;
	
    GetOptions(
               'help|?' => \$help, man => \$man, 
               "connect=s" => \$conn,
               "tablename:s" => \@tname,
               "indexname:s" => \@iname,
	 	       "preview|pretend" => \$preview
               )
        or pod2usage(2);

    
    pod2usage(-msg => $glob_id, -exitstatus => 1) if $help;
    pod2usage(-msg => $glob_id, -exitstatus => 0, -verbose => 2) if $man;

	$glob_preview = $preview;
    $glob_connect = $conn;

    $glob_connect = '-p 11000 template1'
        unless (defined($glob_connect));

    $glob_tname = [];
	push @{$glob_tname}, @tname if (scalar(@tname));

    $glob_iname = [];
	push @{$glob_iname}, @iname if (scalar(@iname));


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


# file fixer: general routine to merge new catalog objects into the
# existing data files
#
# args: hash of new relids
#       sql query to find rows associated with new catalog objects
#       formatted string of relids (for substitution into sql query)
#       name of the column which is a relid foreign key
#       data file name
#       position of the relid key col in the row (0-based)
#       formatted string of column names (vertical bar separator, in order)
sub file_fixer
{
	my ($all_relid, $sql_str, $relid_list, $relid_colname,
		$file_name, $relid_pos, $col_list_str) = @_;

	my $psql_str = "psql ";

    $psql_str .= $glob_connect
        if (defined($glob_connect));

	$sql_str =~ s/\*RELID_LIST\*/$relid_list/g;

    $psql_str .= ' -c " ' . $sql_str .  '"';

    print $psql_str, "\n";

	my $tabdef = `$psql_str`;

    print $tabdef;

    my $rowarr = tablelizer($tabdef);

	my @all_newrows;

	# find all the new rows (matching the relids of the new catalog objects)
    for my $rr (@{$rowarr})
    {
#		print Data::Dumper->Dump([$rr]);

		for my $kk (keys %{$rr})
		{
			# fix NULLs
			$rr->{$kk} = '\N'
				unless (length($rr->{$kk}));
		}

		push @all_newrows, $rr;
#			if (exists($all_relid->{$rr->{$relid_colname}}));
    }

	goto L_previewo if $glob_preview;

	# must be read and writeable
	die ("bad file: $file_name")
		unless (-r $file_name && -w $file_name);

	my $cp2 = "cp $file_name $file_name" . ".old";

	`$cp2`;

	my @all_oldrows;


	open my $file_in, "< $file_name" or die "cannot open $file_name: $!";

	# get the dataset for existing catalog updates and filter out rows
	# which might match the "new" rows...
	for my $ini (<$file_in>)
	{
		next if ($ini =~ m/^\\\./);

		my @foo = split(/\|/, $ini);

#		print Data::Dumper->Dump(\@foo);

		next
			if (exists($all_relid->{$foo[$relid_pos]}));

		push @all_oldrows, $ini;
	}

	close $file_in;


  L_previewo:

	my $file_out;

	if ($glob_preview)
	{
		open $file_out, ">&STDOUT"     or die "Can't dup STDOUT: $!";

		print "\n\n";
	}
	else
	{
		open $file_out, "> $file_name" 
			or die "cannot open $file_name: $!";  
	}

	# save the rows again
	for my $outi (@all_oldrows)
	{

		print $file_out $outi;
	}

	my @col_list = split(/\|/, $col_list_str);

	my $col1 = shift @col_list;

	# add any new rows
	for my $a1 (@all_newrows)
	{
		print $file_out $a1->{$col1};
		for my $cname (@col_list)
		{
			print $file_out "|", $a1->{$cname};
		}
		print $file_out "\n";
	}

	print $file_out "\\.\n";

	print "\n\n" if ($glob_preview);

	close $file_out;
 		

} # end file fixer

if (1)
{
	my $exe = $0;

	# assume current file is in src/test/resgress...
	my $xdir = $exe;
	$xdir =~ s/\/upg2\_wizard\.pl$//;

	my $class_file = "$xdir/data/upgrade34/upg2_pg_class_toadd33.data.in";
	my $attr_file = "$xdir/data/upgrade34/upg2_pg_attribute_toadd33.data.in";
	my $depend_file = "$xdir/data/upgrade34/upg2_pg_depend_toadd33.data";
	my $index_file = "$xdir/data/upgrade34/upg2_pg_index_toadd33.data.in";
	my $type_file = "$xdir/data/upgrade34/upg2_pg_type_toadd33.data.in";
	

    my $psql_str = "psql ";

    $psql_str .= $glob_connect
        if (defined($glob_connect));

	my $sql_str = "select oid, * from pg_class where ";

	my @all_name;

	push @all_name, @{$glob_tname};
	push @all_name, @{$glob_iname};

	my @all_like;

	for my $nam (@all_name)
	{
		push @all_like, " relname like \'".	$nam . "\' ";
	}

	unless (scalar(@all_like))
	{
		die("no table or index names");
		exit(1);
	}

	$sql_str .= join(" or ", @all_like) . " ;" ;

    $psql_str .= ' -c " ' . $sql_str .  '"';

    print $psql_str, "\n";

    my $tabdef = `$psql_str`;

    print $tabdef;

    my $rowarr = tablelizer($tabdef);

    my %reg_tabs;

	my $who_me = `whoami`;

	chomp $who_me;

	my %all_relid;
	my %all_tab;

    for my $rr (@{$rowarr})
    {
		$rr->{relacl} =~ s/$who_me/\@gpcurusername\@/g;

		$rr->{relacl} = '\N'
			unless (length($rr->{relacl}));
		$rr->{reloptions} = '\N'
			unless (length($rr->{reloptions}));
			
#		print Data::Dumper->Dump([$rr]);

		$all_tab{$rr->{relname}} = $rr;
		$all_relid{$rr->{oid}} = $rr->{relname};
    }

#	print Data::Dumper->Dump([%all_relid]);

#	print $class_file, "\n";

	goto L_previewoo if $glob_preview;

	die ("bad file")
		unless (-r $class_file && -w $class_file);

	my $cp1 = "cp $class_file $class_file" . ".old";

	`$cp1`;

	my @all_class;

	open my $class_in, "< $class_file" or die "cannot open $class_file: $!";

	for my $ini (<$class_in>)
	{
		next if ($ini =~ m/^\\\./);

		my @foo = split(/\|/, $ini, 2);

#		print Data::Dumper->Dump(\@foo);

		next
			if (exists($all_relid{$foo[0]}));

		push @all_class, $ini;
	}

	close $class_in;

	L_previewoo:

	my $file_out;

	if ($glob_preview)
	{
		open $file_out, ">&STDOUT"     or die "Can't dup STDOUT: $!";

		print "\n\n";
	}
	else
	{
		open my $file_out, "> $class_file" 
			or die "cannot open $class_file: $!";  
	}

	for my $outi (@all_class)
	{

		print $file_out $outi;
	}

my @class_col_list = split(/\|/, 
"relname|relnamespace|reltype|relowner|relam|relfilenode|reltablespace|relpages|reltuples|reltoastrelid|reltoastidxid|relaosegrelid|relaosegidxid|relhasindex|relisshared|relkind|relstorage|relnatts|relchecks|reltriggers|relukeys|relfkeys|relrefs|relhasoids|relhaspkey|relhasrules|relhassubclass|relfrozenxid|relacl|reloptions"
)	;

	for my $oid (sort {$a <=> $b} keys (%all_relid))
	{
		print $file_out $oid;
		for my $cname (@class_col_list)
		{
			print $file_out "|", $all_tab{$all_relid{$oid}}->{$cname};
		}
		print $file_out "\n";
	}

	print $file_out "\\.\n";

	print "\n\n" if ($glob_preview);

	close $file_out;

	my $relid_list_str = join(",", (sort {$a <=> $b} keys (%all_relid)));

	file_fixer(\%all_relid,
		"select * from pg_attribute where attrelid in " .
		"( *RELID_LIST* ) order by attrelid, attnum;",
			   $relid_list_str,
			   "attrelid",
		$attr_file,  0, 
		"attrelid|attname|atttypid|attstattarget|attlen|attnum|attndims|attcacheoff|atttypmod|attbyval|attstorage|attalign|attnotnull|atthasdef|attisdropped|attislocal|attinhcount"
		);

	# depend

	file_fixer(\%all_relid,
		"select * from pg_depend where refobjid in " .
		"( *RELID_LIST* ) " .
	    " or refobjid in (select oid from pg_type where typrelid in " .
		"( *RELID_LIST* ) ) " .
	    "order by classid, objid, refobjid;",
			   $relid_list_str,
			   "refobjid",
		$depend_file,  4, 
		"classid|objid|objsubid|refclassid|refobjid|refobjsubid|deptype"
		);

	# index

	file_fixer(\%all_relid,
		"select * from pg_index where indexrelid in " .
		"( *RELID_LIST* ) order by indexrelid, indrelid;",
			   $relid_list_str,
			   "indexrelid",
		$index_file,  0, 
			   "indexrelid|indrelid|indnatts|indisunique|indisprimary|indisclustered|indisvalid|indkey|indclass|indexprs|indpred"
		);

	# type

	file_fixer(\%all_relid,
		"select oid,* from pg_type where typrelid in " .
		"( *RELID_LIST* ) order by oid, typname, typrelid;",
			   $relid_list_str,
			   "typrelid",
		$type_file,  9, 
			   "oid|typname|typnamespace|typowner|typlen|typbyval|typtype|typisdefined|typdelim|typrelid|typelem|typinput|typoutput|typreceive|typsend|typanalyze|typalign|typstorage|typnotnull|typbasetype|typtypmod|typndims|typdefaultbin|typdefault"
		);

}


exit();
