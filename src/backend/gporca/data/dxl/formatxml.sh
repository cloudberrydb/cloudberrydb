#!/bin/sh


FORMAT_XML()
{
	QUERY_PATH=`pwd`/$DIRNAME
        for query_file in $QUERY_PATH/*.*
        do
                echo $query_file
		xmllint --format $query_file > '$query_file'
        done

}


DIRNAME=$1

if [ "$DIRNAME" = "" ]
then
	echo "Usage:\n\tsh formatxml.sh  <directory name>"
	exit 0
fi


FORMAT_XML

