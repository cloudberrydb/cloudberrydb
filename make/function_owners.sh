#!/bin/bash

# variables
FILE_OWNER_FILE='file_owners.csv'
FUNCTION_COMPLEXITY_FILE='complexity.csv'
FUNCTION_OWNER_FILE='function_owners.csv'
EXPECTED_ARGS=2
SRC_DIR=$1
OUT_DIR=$2
HEADER='File,Function,owner'

# Check for proper number of command line args
if [ $# -ne $EXPECTED_ARGS ]
then
  echo "Usage: `basename $0` SRC_PATH OUTPUT_PATH"
  exit 1
fi


# store file owner information to OUT_DIR/FILE
find . \(  -name "*.cpp" -o -name "*.c" -o -name "*.h" -o -name "*.inl" \) | \
grep -v libgpdb | \
xargs awk "/^\/\/[ \t]+@owner/ {getline; print substr(FILENAME,3) \",\"\$2}" \
>> "$OUT_DIR"/"$FILE_OWNER_FILE"

# add header to CSV
echo "$HEADER" > "$OUT_DIR"/"$FUNCTION_OWNER_FILE"

# join complexity.csv and file_owners.csv on the first field
join -1 1 -2 1 -o '1.1 1.2 2.2' -t ',' "$OUT_DIR"/"$FUNCTION_COMPLEXITY_FILE" "$OUT_DIR"/"$FILE_OWNER_FILE" \
>> "$OUT_DIR"/"$FUNCTION_OWNER_FILE"
