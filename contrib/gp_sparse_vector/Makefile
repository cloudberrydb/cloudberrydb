#Intel compiler
ifeq "$(CC)" "icc"
	USE_ICC = 1
endif

override CFLAGS+=-std=gnu99

MODULE_big = gp_svec
#Removed the installation SQL file from normal installation
#DATA = gp_svec.sql gp_sfv_test.sql gp_svec_test.sql
DATA = gp_sfv_test.sql gp_svec_test.sql
REGRESS = gp_svec_test.sql
OBJS = gp_sfv.o sparse_vector.o operators.o SparseData.o
ifdef USE_PGXS
	PGXS := $(shell pg_config --pgxs)
	include $(PGXS)
else
	subdir = contrib/gp_sparse_vector
	top_builddir = ../..
	include $(top_builddir)/src/Makefile.global
	include $(top_srcdir)/contrib/contrib-global.mk
endif
#CC=gcc -no-cpp-precomp -m64
#CC=gcc -no-cpp-precomp -m32
#CC=gcc -ftree-vectorizer-verbose=10 -ftree-vectorize
ifdef USE_ICC
	override CFLAGS=-O3 -Werror -std=c99 -vec-report2 -vec-threshold0
endif
