#!/usr/bin/perl

require File::Spec;
use strict;

sub usage
{
    print STDOUT "Usage: $0 <logfile> <outputfile>\n";
}

die &usage() unless $#ARGV == 1;


my $logfile = $ARGV[0];
my $outputfile = $ARGV[1];

my $db = "regression";
my $tmpfile = "/tmp/tmpsqlfile.sql";

die "Input ${logfile} does not exist" unless (-e $logfile);
die "Output ${outputfile} exists!" if (-e $outputfile);

sub runSystem
{
    my $cmd = shift;
    my $ret = system($cmd);
    print STDOUT "return m_bytearray_value of $cmd is $ret\n";
    die "system command $cmd failed" unless $ret == 0;
}

sub runSQLFile
{
    my $sqlfile = shift;

    runSystem("echo \\\\set ON_ERROR_STOP > $tmpfile");
    runSystem("cat $sqlfile >> $tmpfile");
    runSystem("psql $db -a -f ${tmpfile}");
}

sub runSQL
{
    my $sql = shift;

    my $tmpfile = "/tmp/sqlcmd.sql";
    
    open(OUT, ">$tmpfile") or die "Unable to write to tmp file $tmpfile";
    print OUT "$sql";
    close(OUT);

    runSQLFile(${tmpfile});
}


#runSQLFile("setup.sql");

my $logfilepath = File::Spec->rel2abs($logfile);

runSQL("select create_external_table('$logfilepath', 'logs')");

runSQL("drop table if exists stmts");
runSQL("create table stmts (logtime timestamp with time zone, logmessage text)");
runSQL("insert into stmts select logtime, logmessage from logs where logdatabase='regression' and logmessage like 'statement: %'");

my $i1f="/tmp/ifile1.txt";

runSQL("copy (select logmessage from stmts order by logtime) to '${i1f}'");

my $i2f="/tmp/ifile2.txt";

runSystem("cat $i1f | sed 's/\\\\n/ /g' | sed 's/\\\\t/ /g' | sed 's/statement: //g' | grep -i -v stdin | grep -i -v external | grep -i -v gpfdist | egrep ';\$' > ${i2f}");


open(IN, "<${i2f}");
open(OUT, ">$outputfile");

my $queryid = 0;

while ((my $line=<IN>))
{
    chomp($line);

    if ($line =~ m/^select/i 
	&& !($line =~ m/into table/i)
	&& !($line =~ m/interval_tbl/i)
	)
    {
	my $l2 = $line;
	# remove trailing ;
	$l2 =~ s/;$// ;
	$l2 =~ s/'/''/g;

	# print original line
	print OUT "${line}\n";
	print OUT "select run_gpdb_query(\'query${queryid}\',\'$l2\');\n"; 
	print OUT "select run_planfreezer_test(\'query${queryid}\',\'$l2\');\n"; 
	print OUT "select run_algebrizer_test(\'query${queryid}\',\'$l2\');\n";
	print OUT "select run_optimize_test(\'query${queryid}\',\'$l2\');\n";
	
	# for debug purposes
	print OUT "select gpoptutils.RestoreQueryDXL(gpoptutils.DumpQueryDXL(\'$l2\'));\n";

	$queryid = $queryid + 1;
    }
    else
    {
	print OUT "${line}\n";
    }
}


close(OUT);
close(IN);

#runSystem("cp $i2f $outputfile");