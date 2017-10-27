/* $Revision: 1.6 $ */
/*  calculate interpolation weights and vsum */

#include <math.h>
#include "interp.h"

#define min(x,y)  ( (x) < (y) ? (x) : (y) )
#define max(x,y)  ( (x) > (y) ? (x) : (y) )
#define XFAC -0.6931

 binsum( int nf ) {

  register int i, ii, jj, kk, n;
  int lon_180_index;  
  float fac;
  float long_diff_deg, long_diff_rad;
  float west_long_endpoint, east_long_endpoint;

  /* index in array xx (longitude interp  bins) for long. 180 
     in extended portion of array beyond imax */
  if( all_zones )
      lon_180_index = parms.imax + long_extend / 2;
  
  /* determine spatial/temporal interpolation bins for every point */
  for( n=0; n < nf; n++ ) {

      /* longitude bins ranges */
      /* Check for the discontinuities along long. 180. If crossing long. 180
         set ixmin and ixmax to reflect indicies in "extended" portion of 
	 longitude bins in array xx */
      
      west_long_endpoint = x[n] - parms.xwin2;
      east_long_endpoint = x[n] + parms.xwin2;  

      ixmin[n] = (int) floorf( (x[n]-parms.xwin2-parms.xlo)/parms.dx );
      ixmax[n] = (int) floorf( (x[n]+parms.xwin2-parms.xlo)/parms.dx ); 

      /* crossing west of 180 in search and global interp */
      if( west_long_endpoint < -180. && east_long_endpoint > -180. &&
          all_zones ) { 
         
          ixmin[n] = lon_180_index - abs( ixmin[n] );
          ixmax[n] = lon_180_index + ixmax[n];
/* printf( " ixmin, ixmax %d %d in extended array\n", ixmin[n], ixmax[n] ); */
      }

      /* crossing east of 180 in search and global interp */
      else if( west_long_endpoint < 180. && east_long_endpoint > 180. &&
	       all_zones ) {
        
         ixmin[n] = lon_180_index - ( parms.imax - ixmin[n] );
         ixmax[n] = lon_180_index + abs( parms.imax - ixmax[n] );
/* printf( "ixmin, ixmax %d %d in extended array\n", ixmin[n], ixmax[n] ); */
      }
      
      /* no crossing of 180 in search */
      else {         
	 ixmin[n] = max( (int) floorf( (x[n]-parms.xwin2-parms.xlo)/parms.dx ), 0 );
         ixmax[n] = min( (int) floorf( (x[n]+parms.xwin2-parms.xlo)/parms.dx ), parms.imax - 1 );
     }

      /* lat bin ranges */
      iymin[n] = max( (int) floorf( (y[n]-parms.ywin2-parms.ylo)/parms.dy ), 0 );
      iymax[n] = min( (int) floorf( (y[n]+parms.ywin2-parms.ylo)/parms.dy ), parms.jmax - 1 ); 
   
      /* temporal bin ranges */
      izmin[n] = max( (int) floorf( (z[n]-parms.zwin2-parms.zlo)/parms.dz ), 0 );
      izmax[n] = min( (int) floorf( (z[n]+parms.zwin2-parms.zlo)/parms.dz ), parms.kmax - 1 ); 

  }

 
  /* weight the SST and sum */
  for( n=0; n < nf; n++ ) {
    for( kk=izmin[n]; kk <= izmax[n]; kk++ ) {
      for( jj=iymin[n]; jj <= iymax[n]; jj++ ) {
        for( i=ixmin[n]; i <= ixmax[n]; i++ ) {
             
  	/* Derive the guassian weights. Because
	   longitudes can cross 180, e.g., the ranges
	   can be 175 to -175, use the acos(cos( long_diff ))) */
            long_diff_deg = x[n] - xx[i];
            long_diff_rad = acos( cos(deg2rad(long_diff_deg)) );

	    fac = exp( XFAC * ( pow( (rad2deg(long_diff_rad)
                                      /parms.xh),2 ) 
	                    +   pow( ((y[n]-yy[jj])/parms.yh),2 )
    	                    +   pow( ((z[n]-zz[kk])/parms.zh),2 ) ));

            /* if crossing long. 180 must drop weighted SST 
               in correct longitude bin between 0 and imax */        

	    /* range crossing east of 180 */  
            if( i >= lon_180_index && all_zones ) {
                    ii =  i - lon_180_index; 
             
/* printf(" orig bin is %d, correct bin is %d\n", i, ii ); */
            }
            /* range crossing west of 180 */
            else if( (i < lon_180_index && i >= parms.imax)  && all_zones ) {
                    ii = i - long_extend / 2;

/* printf(" orig bin is %d, correct bin is %d\n", i, ii ); */
            }
            /* no crossing */
            else {
	        ii = i;
            }
                 
            vsum[0][ii][jj][kk] += f[n]*fac;
            vsum[1][ii][jj][kk] += fac;
        }
      }
    }
  }
 }









