#include "interp.h"
c $Revision: 2.12 $
c subprogram interp.f
c reads hdf file, extract data, call bisum(), calculates
c and writes out interpolated maps


      subroutine interp( temp, sst_inter, sst_seq, sl, 
     &		  x, y, z, f,
     &            xx, yy, zz, vsum, ixmin, ixmax,
     &		  iymin, iymax, izmin, izmax, ndatamax,
     &		  x_length, y_length, imax, jmax, kmax )

      integer x_length,y_length
      integer dummy

c-- dimension the arrays
      real*4 temp(x_length,y_length)
      real*4 sl(imax,jmax,kmax)
      real*4 sst_inter(kmax), sst_seq(imax)
      real*4 x(ndatamax),y(ndatamax),z(ndatamax),f(ndatamax)
      real*4 xx(imax),yy(jmax),zz(kmax)
      real*4 vsum(2,imax,jmax,kmax)
      real*4 ULlon, ULlat, LRlon, LRlat
      integer*4 ixmin(ndatamax),ixmax(ndatamax)
      integer*4 iymin(ndatamax),iymax(ndatamax)
      integer*4 izmin(ndatamax),izmax(ndatamax)

c-- hdf data array must be hardwired, no dync alloc :(
      byte in_data( XSIZE, YSIZE )

      character*80 plabel
      character*50 infile
      character*30 outfile
      character*150 datafile
      character*30 basename
      character*35 tmpfile
      character*10 mapformat
      character*15 outformat
      character*3 cday
      character*4 cyr
      character*200 zcatcmd

      common /parms/ imin,jmin,kmin,
     &               xwin2,ywin2,zwin2,
     &               xh,yh,zh,
     &               dx,dy,dz,
     &               xlo,xhi,ylo,yhi,zlo,
     &               dxd,dyd

      common /fileio/ infile,outfile,istart,
     &	              iend,mapformat,outformat	

      common /hdfindex/ icolLeft,icolRight,irowUpper,irowLower

      common /basefile/ basename

c-- open output file
      open(unit=20,status='new',form=outformat,file=outfile,
     &     err=1000,iostat=ierr )
      plabel='sst interpolation'

c-- initialize vsum to '0's
      do k1=1,2
        do k2=1,kmax
          do k3=1,jmax
            do k4=1,imax
               vsum(k1,k4,k3,k2)=0.
            enddo
          enddo
        enddo
      enddo

c-- initialize arrays of long, lat and time intervals
      do ii=imin,imax
          xx(ii)=xlo+float(ii-1)*dx  ! Long to interpolate to
      enddo
      do jj=jmin,jmax
          yy(jj)=ylo+float(jj-1)*dy  ! Lat to interpolate to
      enddo
      do kk=kmin,kmax
          zz(kk)=zlo+float(kk-1)*dz  ! Time to interpolate to
      enddo

      do n=1,ndatamax
          x(n)=0.0
          y(n)=0.0
          z(n)=0.0
          f(n)=0.0
	  ixmin(n) = 0
	  ixmax(n) = 0
	  iymin(n) = 0
	  iymax(n) = 0
	  izmin(n) = 0
	  izmax(n) = 0
      enddo

c-- slope (cal) and offset to convert DN to SST
      cal= 0.15
      offset = 3.0

c-- Open input file list
      open(unit=15,status='old',file=infile,access='direct',
     &  recl=RECSIZE,form='formatted',err=1500,iostat=ierr)

c--  read a infile one record at a time and process . . .
      do k=istart,iend
         ipts=0
         read( 15,'(a)',rec=k ) datafile
c	 print *, datafile

        icompress = 0 
c--  hack to get trailing 'Z' (if compressed)
c	do islash = 150, 1, -1
c	    if ( datafile(islash:islash) .eq. 'Z' ) then
c		icompress = 1 
c	        goto 101	
c	    endif
c	enddo
c101     continue

c-- 12/27/99: C call for basename implemented
c-- variable basename returned via common statement
        call getbase( datafile ) 
	print *,'\n file: ',  basename

c--  cyr and cday parsed from basename 
c--  (e.g., parsed from '1988332h54dd-gdm.hdf')
         cday=basename( 5:7 )
         cyr=basename( 1:4 )

c-- if unix compressed, zcat to a temporary filename
        if( basename(22:22) .eq. 'Z' ) then
	    icompress = 1
	    tmpfile = '/tmp/' // basename(1:20)
	    zcatcmd = 'zcat ' // datafile( 1:len(datafile)-1 ) 
     &                        // ' > ' // tmpfile
	    call system( zcatcmd )
	    datafile = tmpfile
	endif

c--  convert iday character to integer
         read(cday,'(i3)') iday
         read(cyr,'(i4)') iyr
c         write(6,*)'cday = ',cday
c         write(6,*)'cyr = ',cyr
          
c***> HDF call: d8gimg() 
         retn=d8gimg(datafile,in_data,x_length,
     &               y_length,dummy)

c--  read hdf DN values and convert to SST
         do  i=icolLeft, icolRight
            do j=irowUpper, irowLower
                xlat=-89.75+dyd*float(j-1)

c-- center output in Pacific Ocean 
c                if( i .lt. x_length ) ix = i-(x_length/2)
c                if( i .le. (x_length/2) ) ix =  i+(x_length/2)

c-- center output in Atlantic (default)
                ix = i

c--  convert signed byte to float
c                if ( in_data(i,j) .lt. 0 .and. abs(xlat) .lt. 70. ) then
                if ( in_data(i,j).lt. 0 ) then
                    temp(ix,j)=float( in_data(i,j) ) + 256
                else
                    temp(ix,j)=float( in_data(i,j) )
                endif



C MULTIPLY THE PATHFINDER DIGITAL NUMBER BY THE CALIBRATION NUMBER (0.15)
C AND ADD THE OFFSET (-3.0) TO GET DEGREES CELSIUS

                 if ( temp(ix,j).gt.0 ) then
                     ipts=ipts+1
                     f(ipts)=( cal*temp(ix,j) ) - offset
                     x(ipts) = ( dxd*float(ix-1) ) - 180. + dxd/2 
                     y(ipts) = ( 90. - (dyd*float(j-1)) ) - dyd/2
                     z(ipts)=float(iday)+float(iyr-1985)*365.
                     if(iyr.gt.1988) z(ipts)=z(ipts)+1
                     if(iyr.gt.1992) z(ipts)=z(ipts)+1
                     if(iyr.gt.1996) z(ipts)=z(ipts)+1
                     if(iyr.gt.2000) z(ipts)=z(ipts)+1
                     if(iyr.gt.2004) z(ipts)=z(ipts)+1
                 endif
            enddo
         enddo

         nfnew=ipts
         print *, ' no of pts in file ',' = ', nfnew

c-- calculate interpolation weights and vsum 
c-- arrays passed directly... a common statement 
c-- does not seem to work.
        call  binsum( nfnew, x, y, z, f, 
     &                xx, yy, zz, vsum,
     &                ixmin, ixmax, iymin,
     &                iymax, izmin, izmax, 
     &                imax, jmax, kmax, ndatamax )

        if( icompress .eq. 1 ) call system( 'rm -f ' // tmpfile )
c-- ..... read next hdf file from infile
      enddo

c-- all input files processed; calculate interpolated SST   
      do kk=1,kmax
        do jj=1,jmax
          do ii=1,imax
            sl(ii,jj,kk)=0.
            if (vsum(2,ii,jj,kk).gt.0) then
                sl(ii,jj,kk)=vsum(1,ii,jj,kk)/vsum(2,ii,jj,kk)
            endif
          enddo
        enddo
      enddo

c-- write output as map "interleaved" or map "sequential"
c-- "interleaved" is the original implementation
c-- both formats preceded by header 

c-- "interleaved" refers to each row in the output
c-- file representing a unique lon, lat position with columns 
c-- of associated sst values (maps) 
c-- i.e. row one: lon1 lat1 sst1 sst2 sst3....lastsst
c--      row two: lon2 lat1 sst1 sst2 sst3....lastsst

c-- "sequential" refers to each map written as rows and
c-- columns of lat, lon with the array element representing the
c-- sst at that geo-position for that map.
c-- each map is written sequentially in the file 

c-- geo-positions of UL and LR corners
          ULlon = -(180 - ( ((icolLeft-1) * dxd) + dxd/2 ))  
	  ULlat = (90 - ( ((irowUpper-1) * dyd) + dyd/2 ))
          LRlon = -(180 - ( (icolRight * dxd) - dxd/2 ))  
	  LRlat = (90 - ( (irowLower * dyd) - dyd/2 ))

c-- version number, "f" refers to fortran version
      version = 'f2.9' 

c-- write the 3 record header
      if( outformat .eq. 'formatted' ) then
          write(20,'(a)'), plabel
          write(20,*) imax,jmax,kmax
          write(20,*) dx,dy,dz 
	  write(20,*) ULlon,ULlat,LRlon,LRlat
      elseif( outformat .eq. 'unformatted' ) then
          write(20) imax,jmax,kmax
          write(20) dx,dy,dz
	  write(20) ULlon,ULlat,LRlon,LRlat
      endif

      if( mapformat .eq. 'interleave' ) then
        print *, '\n map output is interleave'
        do jj=1,jmax
          do ii=1,imax
            do kk=1,kmax
                sst_inter(kk)=sl(ii,jj,kk)
            enddo
            if( outformat .eq. 'formatted' ) then
		write(20,*) ii,jj,sst_inter
            else 
	        write(20) ( sst_inter(i), i=1,kmax ) 
            endif
          enddo
        enddo

      else if( mapformat .eq. 'sequential' ) then
        print *, '\n map output is sequential'
        do kk=1,kmax
          do jj=1,jmax
            do ii=1,imax
               sst_seq(ii)=sl(ii,jj,kk)
             enddo
             if( outformat .eq. 'formatted' ) then
		 write(20,*) jj,kk,sst_seq
             else
		 write(20) ( sst_seq(i), i=1,imax )
             endif
          enddo
        enddo
      endif

      print *,  '\ndone. . . '
      close( 15,status='keep',err=2000,iostat=ierr )
      close( 20,status='keep',err=2500,iostat=ierr )
      stop

 1000 print *, 'Error opening output: ', outfile, 'error num: ', ierr
      goto 102
 1500 print *, 'Error opening input: ', infile, 'error num: ', ierr
      goto 102
 2000 print *, 'Error closing input: ', infile, 'error num: ', ierr
      goto 102
 2500 print *, 'Error closing output: ', outfile, 'error num: ', ierr
      goto 102
 102  continue

      end
