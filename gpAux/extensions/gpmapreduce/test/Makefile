
MAPRED    = ../gpmapreduce
PSQL      = psql
FIXUP     = bin/fixup.pl
MRDIFF    = bin/mrdiff.pl

DB        = mapreduce_test
MFLAGS    = $(DB) --key="clar" --file 
PFLAGS    = -X $(DB) --file

INDIR     = input
DATADIR   = data
MASTERDIR = expected

OUTDIR    = output
YMLDIR    = yml
SQLDIR    = sql

YMLIN     = $(shell ls $(INDIR)/*.yml.in)
YML       = $(notdir $(basename $(YMLIN)))
YMLOUT    = $(addprefix $(YMLDIR)/, $(YML))

TESTS     = $(addprefix $(OUTDIR)/,$(addsuffix .suc, $(basename $(YML))))
MAILDATA  = $(addprefix $(shell pwd)/,$(DATADIR)/email/mailfiles)

all: libs setup $(YMLDIR) $(OUTDIR) $(YMLOUT) $(MAILDATA) $(TESTS)
	@printf "\n=======================\n"
	@if [ -s $(OUTDIR)/FAIL ] ; then \
       export nfailed=`cat $(OUTDIR)/FAIL | wc -l`; \
       export ntotal=`ls -1 $(OUTDIR)/*.suc $(OUTDIR)/*.fail | wc -l`;  \
       printf " %d of %d tests failed. " $$nfailed $$ntotal; \
	 else \
       export ntotal=`ls -1 $(OUTDIR)/*.suc | wc -l`;  \
	   printf " All %d tests passed. " $$ntotal; \
     fi
	@printf "\n=======================\n"

libs:
	$(MAKE) -C lib install

setup:
	@printf "============== dropping database \"$(DB)\"     ==============\n"
	@psql -X -c "drop database $(DB)" template1 2>/dev/null || echo "DROP DATABASE"
	@printf "============== creating database \"$(DB)\"     ==============\n"
	@psql -X -c "create database $(DB)" template1
	@printf "============== installing plperl/plpython             ==============\n"
	@psql -X -c "create language plperl" -d $(DB) 
	@psql -X -c "create language plperlu" -d $(DB) 
	@psql -X -c "create language plpythonu" -d $(DB) 
	@printf "============== running mapreduce tests                ==============\n"
	@rm -rf $(OUTDIR)

clean:
	rm -rf $(YMLDIR)
	rm -rf $(OUTDIR)
	rm -rf $(MAILDATA)
	@dropdb $(DB) 2>/dev/null || echo > /dev/null


%.test: $(OUTDIR)/%.suc

$(OUTDIR):
	@mkdir $(OUTDIR)

$(MAILDATA):
	@ls -1 $(dir $(MAILDATA))*.txt > $(MAILDATA)
	@chmod u+w $(MAILDATA)

$(YMLDIR):
	@mkdir $(YMLDIR)

$(YMLDIR)/%.yml: $(INDIR)/%.yml.in
	@$(FIXUP) $(INDIR)/$*.yml.in
	@mv $(INDIR)/$*.yml $(YMLDIR)/.

$(OUTDIR)/%.suc: $(MAPRED) $(YMLDIR)/%.yml $(MASTERDIR)/%.out $(MASTERDIR)/%.err
	@printf 'test %-20s ... ' $*
	@rm -f $(OUTDIR)/$*.suc $(OUTDIR)/$*.fail
	@touch $(OUTDIR)/$*.out $(OUTDIR)/$*.err
	@cp -f $(MASTERDIR)/DEFAULT.out $(OUTDIR)/$*.out
	@cp -f $(MASTERDIR)/DEFAULT.err $(OUTDIR)/$*.err
	@if [ -e $(SQLDIR)/$*_init.sql ] ; \
	 then $(PSQL) $(PFLAGS) $(SQLDIR)/$*_init.sql > $(OUTDIR)/$*.init.out; fi
	@$(MAPRED) $(MFLAGS) $(YMLDIR)/$*.yml >>$(OUTDIR)/$*.out 2>>$(OUTDIR)/$*.err; \
		if [ $$? -ne 0 ]; then echo "gpmapreduce retcode=$$?" >> $(OUTDIR)/$*.err; fi
	@if [ -e $(SQLDIR)/$*_done.sql ] ; \
	 then $(PSQL) $(PFLAGS) $(SQLDIR)/$*_done.sql > $(OUTDIR)/$*.done.out 2>&1; fi
	@$(MRDIFF) $(OUTDIR) $(MASTERDIR) $* > $(OUTDIR)/$*.suc
	@psql -c "\d" $(DB) | \
        perl -e "while (<>) {s/No relations found.\\n$$//; print}" >> $(OUTDIR)/$*.suc
	@cd $(OUTDIR); if [ -s $*.suc ] ; then \
	    mv $*.suc $*.fail; echo FAILED; echo $*.fail >> FAIL ; else echo ok; fi


# If the master files don't exist this won't let the test pass, but it will
# allow the test to run
$(MASTERDIR)/%.out:
	@echo "FILE MISSING" > $@

$(MASTERDIR)/%.err:
	@echo "FILE MISSING" > $@
