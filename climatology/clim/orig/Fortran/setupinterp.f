#include "interp.h"
c $Revision: 2.12 $
c NAME gaussinterp
c 7 Dec 99 	earmstrong 	NASA/JPL
c
c DESCRIPTION
c Gaussian interpolation program modified to map SST (HDF) data.
c Modified from intersstdbg7day3.f (jvazquez' prog).  
c
c SYNOPSIS
c setupinterp() allocates space for arrays, calls interp() which does the 
c interpolation
c  
c USAGE
c % gaussinterp infile beg_rec end_rec outfile parmfile

c **** Original program comments (intersstdbg7day3.f)  ***
c DESCRIPTION: Maps data when given the required information. Program
c   originally written by raghu. Version 7 reads the gridded data set.
c   Currently maps TMR water vapor.
c
c COMMENTS:
c  This programs wraps around in longitude for 0 to 360 maps.
c  If different maps size is needed in longitude w/o wraparound
c  some lines of code need to be modified in the file.
c
c
c CHANGES:
c  2/6/97 - change fint from integer*4 to integer*2; holds residuals
c 2/19/97 - change output to direct access file, change sl() to int*2
c 8/8/97 - change i/o so that it reads data directly from the residual
c          data files
c 9/9/97 - change main program so that i/o part are subroutines
c 9/10/97 - add header,time,lat,lon and #of maps to output file
c 9/11/97 - add version and output file specification in runtime
c           argument list
c **** end Orginal comments ****

c 12/01/99 - major modifications (ema). 
c	   	- added command line read for input, output filenames
c		  and records numbers of input file read
c		- binsum is now a subprogram
c		- mapping params (interp.h) read in with include statement
c		- removed some superfluous do loops
c 12/06/99 - dynamic allocation of arrays added
c	   - read input parms from arg 5 (see 'interp.parms'
c	     for an example of format)
c	   - hdf data array size still hardcoded in interp.h
c 12/10/99 - variable zlo set equal to command line arg for first
c	     record number in infile to process (istart). zlo 
c	     removed from parameter file (interp.parms)
c 12/13/99 - imax, jmax calculated from xlo, xhi, ylo, and yhi
c	     imin, jmin, kmin set to 1
c 12/16/99 - added options in parameter file for sequential
c	     or interleaved maps, and formatted or unformatted
c	     output file
c 12/27/99 - added basename C call. program can now handle 
c            compressed datafiles.
c 1/4/00   - added subsetting of hdf array based on lon/lat 
c	     bounds. zlo now read from parameter file. 
c 1/4/00   - zlo found from parse of first datafile.
c	     geo-positions of UL, LR written to output header

      program setupinterp
      
      integer ndatamax, x_length, y_length
      integer temp_malloc, sst_inter_malloc
      integer sst_seq_malloc, sl_malloc
      integer x_malloc, y_malloc, z_malloc, f_malloc
      integer xx_malloc, yy_malloc, zz_malloc, vsum_malloc
      integer ixmin_malloc, ixmax_malloc
      integer iymin_malloc, iymax_malloc
      integer izmin_malloc, izmax_malloc
      real*4 float_size
      integer*4 int_size

      character*80 plabel
      character*50 infile
      character*30 outfile
      character*60 parmfile
      character*10 mapformat
      character*15 outformat
      character*6 startrec
      character*6 endrec
      character*3 cday
      character*4 cyr
      character*150 datafile
      character*30 basename

      common /parms/imin,jmin,kmin,
     &	             xwin2,ywin2,zwin2,
     &		     xh,yh,zh,		
     &		     dx,dy,dz,
     &		     xlo,xhi,ylo,yhi,zlo,
     &		     dxd,dyd

      common /fileio/ infile,outfile,istart,
     &	              iend,mapformat,outformat	
      common /hdfindex/ icolLeft,icolRight,irowUpper,irowLower
       
      common /basefile/ basename

c ...check command line arguments
      call getarg( 1, infile )
      call getarg( 2, startrec )
      call getarg( 3, endrec )
      call getarg( 4, parmfile )
      call getarg( 5, outfile )

      inum_args = iargc()
      if ( inum_args .ne. 5 ) then
	  call usage()
	  stop
      endif

      read( startrec, '(i)' ) istart
      read( endrec, '(i)' ) iend

      if ( iend .lt. istart ) then
	  write(6,*) '\n	Error: end record number
     & smaller than beginning record ! \n'
	  stop
      endif

c--  zlo determined by reading first record and parsing the datafile
c--  for year and julian day using getbase()  (C call)

c-- Open input file list
      open( unit=ifile,status='old',file=infile,access='direct',
     &  recl=RECSIZE,form='formatted',err=2000,iostat=ierr )

       read( ifile, '(a)', rec=istart ) datafile
       close( ifile, status='keep', err=2500, iostat=ierr )

       call getbase( datafile )	! returns basename
       cday=basename( 5:7 )
       cyr=basename( 1:4 )
       read( cday,'(i3)' ) iday
       read( cyr,'(i4)' ) iyr
       zlo = float(iday)+float(iyr-1985)*365.
       if(iyr.gt.1988) zlo = zlo + 1.
       if(iyr.gt.1992) zlo = zlo + 1.
       if(iyr.gt.1996) zlo = zlo + 1.
       if(iyr.gt.2000) zlo = zlo + 1.
       if(iyr.gt.2004) zlo = zlo + 1.
       print *, 'zlo is ', zlo

c-- read in the parameter file 
      open( unit=iparm,status='old',form='formatted',file=parmfile,
     &     err=1000,iostat=ierr )

      read( unit=iparm, fmt=* ) ndatamax
      read( unit=iparm, fmt=* ) x_length, y_length
      read( unit=iparm, fmt=* ) kmax
      read( unit=iparm, fmt=* ) xwin2, ywin2, zwin2
      read( unit=iparm, fmt=* ) xh, yh, zh
      read( unit=iparm, fmt=* ) dx, dy, dz
      read( unit=iparm, fmt=* ) xlo, xhi
      read( unit=iparm, fmt=* ) ylo, yhi
c      read( unit=iparm, fmt=* ) zlo
c      read( unit=iparm, fmt=* ) dxd, dyd
      read( unit=iparm, fmt='(a)' ) mapformat
      read( unit=iparm, fmt='(a)' ) outformat
      close( iparm,status='keep',err=1500,iostat=ierr )

c	 write(*,*) ndatamax
c	 write(*,*) x_length, y_length
c	 write(*,*) kmax
c	 write(*,*) xwin2,ywin2, zwin2
c	 write(*,*) xh, yh, zh
c	 write(*,*) dx, dy, dz
c	 write(*,*) xlo, xhi
c	 write(*,*) ylo, yhi
c	 write(*,*) zlo
cc	 write(*,*) dxd, dyd
c	 write(*,*) mapformat
c	 write(*,*) outformat

      if( (mapformat .ne. 'interleave') .and.  
     &    (mapformat .ne. 'sequential') ) then
	   print *, 'check parameter file for interleave or sequential maps !'
	   stop
      endif
      if( (outformat .ne. 'formatted') .and.  
     &    (outformat .ne. 'unformatted') ) then
	   print *, 'check parameter file for formatted or unformatted output !'
	   stop
      endif


c-- calculate max number of interpolation "steps" based on
c-- long and lat ranges divided by interpolation interval
c      imax = ( (xhi - xlo) + 1 ) / dx
c      jmax = ( abs(yhi - ylo) + 1 ) / dy
      imax = ( abs(xhi - xlo)  ) / dx
      jmax = ( abs(yhi - ylo)  ) / dy

      print *, 'imax, jmax, kmax',  imax,jmax,kmax

      imin = 1
      jmin = 1
      kmin = 1

c-- calculate degrees per grid point
      dxd =  360. / float(x_length)
      dyd =  180. / float(y_length)

c-- find columns and rows for subsetting into hdf image array 
c-- remember @ hdfarray(1,1) -->  lon=-180, lat=90
      icolLeft = nint( ((xlo + 180) / dxd) + 1 )
      icolRight = nint( (xhi + 180) / dxd )
      irowUpper = nint( (abs(yhi - 90) / dyd) + 1 )
      irowLower = nint( (abs(ylo - 90) / dyd) )

c      print *, 'icolLeft, icolRight,irowUpper, irowLower', 
c     & icolLeft, icolRight, irowUpper, irowLower 

c-- 4 bytes for floats and ints
      float_size = 4
      int_size = 4

c-- allocate space for arrays
          itotsize = x_length*y_length*float_size
      temp_malloc = malloc( %val(itotsize) )     
          itotsize = kmax*float_size
      sst_inter_malloc = malloc( %val(itotsize) )     
          itotsize = imax*float_size
      sst_seq_malloc = malloc( %val(itotsize) )     
          itotsize = imax*jmax*kmax*float_size
      sl_malloc = malloc( %val(itotsize) ) 
          itotsize =  ndatamax*float_size
      x_malloc = malloc( %val(itotsize) ) 
      y_malloc = malloc( %val(itotsize) ) 
      z_malloc = malloc( %val(itotsize) ) 
      f_malloc = malloc( %val(itotsize) ) 
          itotsize = imax*float_size
      xx_malloc = malloc( %val(itotsize) ) 
          itotsize = jmax*float_size
      yy_malloc = malloc( %val(itotsize) ) 
          itotsize = kmax*float_size
      zz_malloc = malloc( %val(itotsize) ) 
          itotsize = 2*imax*jmax*kmax*float_size
      vsum_malloc = malloc( %val(itotsize) ) 
          itotsize = ndatamax*int_size
      ixmin_malloc = malloc( %val(itotsize) ) 
      ixmax_malloc = malloc( %val(itotsize) ) 
      iymin_malloc = malloc( %val(itotsize) ) 
      iymax_malloc = malloc( %val(itotsize) ) 
      izmin_malloc = malloc( %val(itotsize) ) 
      izmax_malloc = malloc( %val(itotsize) ) 

c      print *, 'done malloc arrays\n'

c--- call interp() with 'space' variables passed call by value
      call interp( %val(temp_malloc), 
     &      %val(sst_inter_malloc), %val(sst_seq_malloc),
     &      %val(sl_malloc), %val(x_malloc), %val(y_malloc), 
     &	    %val(z_malloc), %val(f_malloc), %val(xx_malloc),
     &	    %val(yy_malloc), %val(zz_malloc), 
     &	    %val(vsum_malloc), %val(ixmin_malloc), %val(ixmax_malloc),
     &	    %val(iymin_malloc), %val(iymax_malloc), %val(izmin_malloc), 
     &	    %val(izmax_malloc), ndatamax, x_length, y_length,
     &	    imax, jmax, kmax )

c-- how to free memory? dunno


 1000 print *, 'Error opening parameter file: ', parmfile, 'error num: ', ierr
 1500 print *, 'Error closing parameter file: ', parmfile, 'error num: ', ierr
 2000 print *, 'Error opening input: ', infile, 'error num: ', ierr
 2500 print *, 'Error closing input: ', infile, 'error num: ', ierr

      end

      subroutine usage()
	  print *, '\n	~~ gaussian interpolation of pathfinder SST data
     & (f77 version) ~~\n '
	  print *, 'USAGE: % gaussinterp infile beg_rec end_rec parmfile outfile\n'
	  print *, ' -- infile: file containing list to process
     & (ascii; fixed length records)'
	  print *, ' -- beg_rec: first record (file) in infile processed'
	  print *, ' -- end_rec: last record (file) in infile processed'
	  print *, ' -- parmfile: input parameter file (e.g., interp.parms)'
	  print *, ' -- outfile: interpolated output file (ascii or binary)'
	  print *, '\n e.g., % gaussinterp list.dat 100 600 interp.parms output.dat '
	  print *, '[process records 100 to 600 of list.dat; interpolated
     & result is output.dat]\n'
	  print *, 'note: see interp.parms for example format of parameter file'
	  print *, '      see gaussinterp.readme for more general information'
	  print *, '      this executable compiled for image x and y size:', XSIZE, YSIZE  
      end
