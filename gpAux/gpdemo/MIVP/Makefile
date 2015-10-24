THIS_PGDATABASE=gptestdb
THIS_PGPORT=15432

DATADIR=$(PWD)/data-generator

.PHONY : create-IVP generate-data load-data drop-IVP query-IVP

all : drop-IVP create-IVP generate-data load-data query-IVP
	@echo "MIVP: load and query test completed successfully."

query-IVP :
	@echo ""
	@echo "Executing step \"query-IVP\""
	@echo ""
	@echo "The result from this next query should be 1000000"
	@echo
	psql -p $(THIS_PGPORT) $(THIS_PGDATABASE) -c "select count(*) from bigtable1"

drop-IVP :
	@echo ""
	@echo "Executing step \"drop-IVP\""
	@echo ""
	@-psql -p $(THIS_PGPORT) -c "DROP DATABASE IF EXISTS $(THIS_PGDATABASE)" template1

create-IVP :
	@echo ""
	@echo "Executing step \"create-IVP\""
	@echo ""
	@psql -p $(THIS_PGPORT) -c "CREATE DATABASE $(THIS_PGDATABASE)" template1
	@psql -p $(THIS_PGPORT) $(THIS_PGDATABASE) -c "create table bigtable1 ( a timestamp, b varchar, c varchar, d integer, e integer, f integer, g integer, h numeric, i varchar, j varchar, k varchar, l integer, m integer, n varchar, o varchar )"

generate-data :
	@echo ""
	@echo "Executing step \"generate-data\""
	@echo ""
	cd $(DATADIR); make

load-data :
	@echo ""
	@echo "Executing step \"load-data\""
	@echo ""
	@echo "COPY bigtable1 (a,b,c,d,e,f,g,h,i,j,k,l,m,n,o) FROM '$(DATADIR)/dbdata.txt' WITH DELIMITER '|';" > copy.ctl
	@psql -p $(THIS_PGPORT) $(THIS_PGDATABASE) -f "copy.ctl"

clean :
	@rm -f copy.ctl
	@-make drop-IVP
	@-cd $(DATADIR);make clean
