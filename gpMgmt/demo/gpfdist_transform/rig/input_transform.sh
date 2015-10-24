#!/bin/bash
#
# input_transform.sh 
# sample input transformation, demonstrating use of Java and Joost STX
# to convert some WITSML XML into escaped text suitable for loading into GPDB.
#
# java arguments:
#   -jar data/joost.jar       joost STX engine
#   -nodecl                   don't generate an <?xml?> declaration
#   $1 (or -)                 filename to process (or stdin)
#   rig/input_transform.stx   STX transformation
#
# This first step extracts interesting attributes from the input document
#
java \
    -jar data/joost.jar \
    -nodecl \
    $1 \
    rig/input_transform.stx | \
awk 'NF>0 {printf "%s|", $0}'

# This second awk command escapes newlines in the input document before
# sending it to GPDB so that the entire document ends up in our xml 'doc' column.
#
awk '{ printf "%s\\n",$0 }' $1
