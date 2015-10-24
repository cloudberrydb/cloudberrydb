#!/bin/bash

for queryfile in *.java
do
	query=`basename ${queryfile} .java`
	echo "Compiling: ${queryfile}"
	javac $queryfile
	echo 'Running: ${query}'
	java ${query} > ${query}.out
done

