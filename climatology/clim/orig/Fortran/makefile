# makefile for guassinterp. 
# choose f77 or f90 compiler 
# and appropriate flags 

# f77 compiler 
FC  = f77
FFLAGS = -g    
#FFLAGS = -O -n32   
DEFINE =  -DLANG_F77
HDF = /usr/local/hdf
#HDF = /usr/local/hdf4.1r3-n32

# f90 compiler
#FC = f90
#FFLAGS = -n32 -bytereclen -cpp -extend_source
#DEFINE =  -DLANG_F90

PGM = gaussinterp
LIBS    = -L$(HDF)/lib -ldf -ljpeg -lz -lgen
INCLUDE = -Wf,-I/usr/local/include/hdf
BIN = /sst/vol7/PATHFINDER/bin/sgi

SRC = setupinterp.f interp.f binsum.f getbase.c passbase.f
OBJ = setupinterp.o interp.o binsum.o getbase.o passbase.o

$(PGM):	$(OBJ)
	$(FC) $(FFLAGS) -o $(PGM) $(OBJ) $(LIBS) 

setupinterp.o: 
	$(FC) $(FFLAGS) $(DEFINE) -c setupinterp.f
interp.o:
#	$(FC) $(DEFINE) -c interp.f
	$(FC) $(FFLAGS) $(DEFINE) -c interp.f
binsum.o:
	$(FC) $(FFLAGS) $(DEFINE) -c binsum.f
passbase.o:
	$(FC) $(FFLAGS) $(DEFINE) -c passbase.f
getbase.o:
	$(CC) $(FFLAGS) -c getbase.c

install: $(PGM)
	 cp  $(PGM) $(BIN)

clean:
	 rm -f $(OBJ) $(PGM)

