CC=gcc
OBJS=main.o utils.o

EXEC=generator

DATAFILE=dbdata.txt
SEEDFILE=seedfile.txt
NRECORDS=1000000

COPTIONS=-O3
#COPTIONS=-O3 -m64
#COPTIONS=-g

.c.o:
	$(CC) $(COPTIONS) -c $*.c

all: $(OBJS)
	$(CC) -o $(EXEC) $(COPTIONS) $(OBJS)	
	./$(EXEC) $(SEEDFILE) $(NRECORDS) > $(DATAFILE)

clean:
	@rm -f $(OBJS)
	@rm -f $(EXEC)
	@rm -f $(DATAFILE)
