.PHONY: all test

RM         = $(shell which rm)
MKDIR      = $(shell which mkdir)
DIFF       = $(shell which diff)
ECHO       = $(shell which echo)
DBBIN      = $(shell pg_config --bindir)
CHECK_CONF = $(shell pwd)/bin/check_config.sh
LAUNCH_RD  = $(shell pwd)/bin/launch_read.sh
LAUNCH_WR  = $(shell pwd)/bin/launch_write.sh
CREATELANG = $(DBBIN)/createlang
PSQL       = $(DBBIN)/psql
SAS_READ   = $(DBBIN)/sas_read
SAS_WRITE  = $(DBBIN)/sas_write
SQLFLAGS   = -X

RESULTDIR  = results

test: all

clean: 
	@$(RM) -fr $(RESULTDIR)

all: clean
	@$(CHECK_CONF)
	@$(MKDIR) results
	@$(CREATELANG) plpythonu | echo "Loading plpythonu"
	@$(ECHO) "Creating External Tables"
	@$(PSQL) $(SQLFLAGS) -f ../sql/setup.sql > $(RESULTDIR)/setup.out
	@$(LAUNCH_RD)
	@$(ECHO) "Testing Read Query"
	@$(PSQL) $(SQLFLAGS) -f ../sql/read.sql  > $(RESULTDIR)/read.out
	@$(LAUNCH_WR)
	@$(ECHO) "Testing Write Query"
	@$(PSQL) $(SQLFLAGS) -f ../sql/write.sql > $(RESULTDIR)/write.out
	@$(ECHO) "Checking Results"
	@$(PSQL) $(SQLFLAGS) -f ../sql/check.sql > $(RESULTDIR)/check.out
	@$(DIFF) -qs $(RESULTDIR)/check.out expected/check.out
