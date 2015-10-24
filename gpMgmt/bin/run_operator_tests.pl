#!/usr/bin/env perl

####################################################
#Author  : Bharath
#Usage   : sample usage > perl run_operator_tests.pl -port <Master port number> -dbname <Name of the database with tpch data> -outfile <file where the output of queries is written> -runtime <File into which the query runtimes will be written>

#Notes  :  The program consolidates all the sql files present in 'opperf' directory into a single flat file, executest the queries and later on extracts the timing information for each of the queries
###################################################

#use strict;
use warnings;
use Getopt::Long;

# Arguments to the program
my ($port, $dbname, $outfile, $runtimes);

GetOptions(
        'port=i' => \$port,
	'dbname=s' => \$dbname,
        'outfile=s' => \$outfile,
        'runtime=s' => \$runtime 
	);

unless (defined($port) && defined($dbname) && defined($outfile) && defined($runtime))
{
    print "\nUsage: $0 -port <master port number> -dbname <Name of the database with tpch data> -outfile <File where the output of queries is written> -runtime <File into which the query runtimes will be written>\n\n";

    print "Notes: The program consolidates all the sql files present in 'opperf' directory into a single flat file, executes the queries and later on extracts the timing information for each of the queries and writes it to 'runtime file'\n\n".

    exit;
}

my $sqlfile = "/tmp/operator_perf_tests_".`date "+%Y%m%d"`;
chomp($sqlfile);

#Gather all the sqls in individual sql files in opperf directory into a temporary file

open(OUT,">$sqlfile") or die("Unable to create file: $sqlfile\n");
print OUT "\\timing\n";
close(OUT);

system("cat opperf/*.sql >> $sqlfile");

print "\nExecuting the queries in $sqlfile \n\n";
print "The output can be viewed in file: $outfile\n";

system("psql -p $port $dbname -a -f $sqlfile > $outfile 2>&1");

print "\nDone with executing the queries......\n\n";

print "Extracting the run times of the queries:";

open(IN,"<$outfile") or die("Unable to open file $outfile\n");

open(OUT,">$runtime") or die("Unable to open file $runtime for writing");

my $query_number=1;
my $guc_setting=0; #variable used to indicate if the previous statement was a GUC setting

while($line=<IN>)
{
	chomp($line);
	if($line=~/^Time:\s+(\d+\.\d+)\s+ms$/) #Line with timing information
        {
		if(not $guc_setting) #We don't care about the timings returned for GUC stmts	
		{
			print OUT "Query-$query_number: $1 ms\n";
 			$query_number++;
		}
 		next;
   	} 
	if($line=~/^SET$/)
	{
		$guc_setting = 1;
	}
        else
	{
		$guc_setting = 0;
 	}
}

print " Done.\n\nThe timing information is available in file: $runtime\n";
