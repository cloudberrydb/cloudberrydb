#!/bin/bash


for query in *.java
do
	sed s/gptest/template1/g $query > ${query}.1 ; mv ${query}.1 ${query}
done

