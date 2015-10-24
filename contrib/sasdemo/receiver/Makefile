PROGRAM    = sasreceiver
OBJS       = sasreceiver.o
CC         = gcc
CFLAGS     = -O2 -Wall
LIBS       = -lc

INSTALLDIR = $(shell pg_config --bindir)


all: $(PROGRAM)

clean:
	rm -f $(PROGRAM) $(OBJS)

install: all
	cp $(PROGRAM) $(INSTALLDIR)/$(PROGRAM)

uninstall:
	rm -f $(INSTALLDIR)/$(PROGRAM)

$(PROGRAM): $(OBJS)
	$(CC) -o $@ $(OBJS) $(LIBS)

%.o: %.c
	$(CC) $(CFLAGS) -c $<


