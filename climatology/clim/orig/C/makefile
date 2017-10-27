# makefile for guassinterp (C version). 

# C compiler 
CC  =  cc
#HDF = /usr/local/hdf
HDF = /usr/local/hdf4.1r3-n32

CFLAGS =  -n32 -I$(HDF)/include  -O -w
#LIBS    = -L$(HDF)/lib -lmfhdf -ldf -ljpeg -lz -lgen -lm 
LIBS    = -L$(HDF)/lib  -ldf -ljpeg -lz -lgen -lm 
BIN = /sst/vol7/PATHFINDER/bin/sgi
PGM = gaussinterp

SRC = setupinterp.c interp.c binsum.c 
OBJ = setupinterp.o interp.o binsum.o 

$(PGM):	$(OBJ)
	$(CC) $(CFLAGS) -o $(PGM) $(OBJ) $(LIBS) 

setupinterp.o: 
	$(CC) $(CFLAGS) $(DEFINE) -c setupinterp.c
interp.o:
	$(CC) $(CFLAGS) $(DEFINE) -c interp.c
binsum.o:
	$(CC) $(CFLAGS) $(DEFINE) -c binsum.c


install: $(PGM)
	 cp  $(PGM) $(BIN)

clean:
	 rm -f $(OBJ) $(PGM)

