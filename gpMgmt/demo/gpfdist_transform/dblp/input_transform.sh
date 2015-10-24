#!/bin/bash
#
# input_transform.sh 
# sample input transformation, demonstrating use of Java and Joost STX
# to convert XML into escaped text suitable for loading into GPDB.
#
# java arguments:
#   -jar data/joost.jar       joost STX engine
#   -nodecl                   don't generate an <?xml?> declaration
#   $1 (or -)                 filename to process (or stdin)
#   dblp/input_transform.stx  STX transformation
#
# note we use an extra AWK step at the end because joost likes to 
# emit a blank line at the end.

java \
    -jar data/joost.jar \
    -nodecl \
    $1 \
    dblp/input_transform.stx | \
awk 'NF>0'
