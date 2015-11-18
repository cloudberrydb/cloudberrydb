#!/bin/bash

# variables
FILE='complexity.csv'
EXPECTED_ARGS=2
SRC_DIR=$1
OUT_DIR=$2
OFST='3'
MAX_COMPLEX='10'
HEADER='File,Function,Modified McCabe Complexity,McCabe Complexity,Statements,Lines'
CSV_FORMAT="{OFS=\",\"; print substr(\$6, $OFST,index(\$6,\"(\") - $OFST),\$7,\$1,\$2,\$3,\$5; }"
ERROR="NR!=1 {FS=\",\"; if (\$3 > $MAX_COMPLEX) print \"ERROR: function \'\"\$2\"\' has complexity \"\$3;}"
ERROR_EXIT="NR!=1 {FS=\",\"; if (\$3 > $MAX_COMPLEX) exit(1);}"

# Check for proper number of command line args
if [ $# -ne $EXPECTED_ARGS ]
then
  echo "Usage: `basename $0` SRC_PATH OUTPUT_PATH"
  exit 1
fi

# add header to CSV
echo "$HEADER" > "$OUT_DIR"/"$FILE"

# store complexity information to OUT_DIR/FILE
find $SRC_DIR \(  -name "*.cpp" -o -name "*.c" -o -name "*.h" -o -name "*.inl" \)  -print | \
grep -v libgpdb | \
xargs pmccabe 2> /dev/null | \
awk "$CSV_FORMAT" \
>> "$OUT_DIR"/"$FILE"

# error out for functions with complexity greater than MAX_COMPLEX
awk "$ERROR" < "$OUT_DIR"/"$FILE"
awk "$ERROR_EXIT" < "$OUT_DIR"/"$FILE" 
