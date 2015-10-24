#!/bin/bash
#
# output_transform.sh 
# sample output transformation demonstrating use of awk to convert
# tab separated output from GPDB back into simple xml
#
awk '
BEGIN {
    FS="\t"
    print "<?xml version=\"1.0\"?>"
    print "<dblp>"
}
{
    print "<" $2 "thesis key=\"" $1 "\">"
    print "<author>" $3 "</author>"
    print "<title>" $4 "</title>"
    print "<year>" $5 "</year>"
    print "<school>" $6 "</school>"
    print "</" $2 "thesis>"
    print ""
}
END {
    print "</dblp>"
}' > $*
