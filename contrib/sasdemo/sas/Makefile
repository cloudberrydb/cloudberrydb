PROGRAMS  = sas_read sas_write
OBJS      = sas_read.o sas_write.o
CC        = gcc
CFLAGS    = -O2 -Wall
LIBS      = -lc

INSTALLDIR = $(shell pg_config --bindir)


all: $(PROGRAMS)

clean:
	rm -f $(PROGRAMS) $(OBJS)

install: all
	cp $(PROGRAMS) $(INSTALLDIR)/.

uninstall:
	rm -f $(INSTALLDIR)/sas_read $(INSTALLDIR)/sas_write

sas_read: sas_read.o
	$(CC) -o $@ $< $(LIBS)

sas_write: sas_write.o
	$(CC) -o $@ $< $(LIBS)

%.o: %.c
	$(CC) $(CFLAGS) -c $<
