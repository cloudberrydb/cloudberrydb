\! postgres --help &> /dev/null
\! postgres --gp-version &> /dev/null
\! postgres --catalog-version &> /dev/null
\! postgres --describe-config &> /dev/null
\! postgres --version &> /dev/null
\! createlang --help &> /dev/null
\! createdb --help &> /dev/null
\! createdb --version &> /dev/null
\! createdb jsoedomo &> /dev/null
\! createdb jsoedomo &> /dev/null
\! createdb jsoedomo2 'Johnny Soedomo' &> /dev/null
\! createdb -e jsoedomo3 'Johnny Soedomo &> /dev/null
\! createdb jsoedomo4 -E UUTT &> /dev/null
\! createdb jsoedomo4 -E &> /dev/null
\! dropdb --help &> /dev/null
\! dropdb --version &> /dev/null 
\! dropdb jsoedomo &> /dev/null
\! dropdb jsoedomo &> /dev/null
\! dropdb jsoedomo2 &> /dev/null
\! dropdb -e jsoedomo3 &> /dev/null
\! dropdb jsoedomo4 -E &> /dev/null
\! psql --help &> /dev/null 
\! psql regression -c '\?' &> /dev/null
\! psql -U xxx regression &> /dev/null
\! psql -d regression -f sql/largeobject.sql &> /dev/null
\! gpconfig -c gp_resqueue_priority -v on &> /dev/null
\! gpstop -ar &> /dev/null
\! psql -d regression -f sql/optquery.sql &> /dev/null
\! gptorment.pl -connect '-p $PGPORT -d template' -parallel 3 -sqlfile sql/optquery.sql &> /dev/null
\! gpconfig -c gp_resqueue_priority -v off &> /dev/null
\! gpstop -ar &> /dev/null
\! psql -d regression -f sql/triggers.sql &> /dev/null
\! psql -d regression -f sql/mpp-11333.sql &> /dev/null
