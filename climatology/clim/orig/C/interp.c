/* $Revision: 1.20 $ */

/* SYNOPSIS
   called by setupinterp(). This subroutine reads each
   HDF file, passes data to binsum() for interpolation
   calculation and writes output image(s).
*/

#include <stdio.h>
#include <string.h>
#include <libgen.h>
#include <hdf.h>
#include "interp.h"

/* C binary file id = 3890345459 (0xE7E1F5F3) */
#define MAGIC 3890345459L

 interp() {
  uint8 in_data[parms.y_length][parms.x_length];   
  uint8 quality_data[parms.y_length][parms.x_length];   
  uint8 aerosol_corr[parms.y_length][parms.x_length];   

  intn retn, haspal;
  int32 width, height;

  float ULlon, ULlat, LRlon, LRlat;
  float cal, offset, xlat;
  int k1,k2,k3,k4,ii,jj,kk,n,i,j,k;
  int ipts, iyr, iday;
  int jj_reverse;
  int row, col;
  unsigned long int magic_num; 

  FILE *fid_infile, *fid_outfile, *fid_datafile;
  FILE *fid_aerosol_list, *fid_aerosol_file;

  char plabel[40];
  char datafile[RECSIZE];
  char aerosol_file[200];
  char *basefile;
  char cday[3];
  char cyr[4];
  char tmpstr[22]; 
  char aerosol_tmpstr[30];
  char tmpfile[35];
  char aerosol_tmpfile[45];
  char zcatcmd[200];
  char rmcmd[45];
  char compress[1];
  char version[4];
  char sstDN;
  /* short int sstDN; */

  /* open output file */
  fid_outfile = fopen( fileio.outfile, "w" );


  /* Set arrays of interpolation bins.
     Long and lat bin centers set to 1/2 
     interpolation step */

  /*  Long to interpolate to:
      1st for loop sets interpolation bins
      between the ranges of xlo to xhi.
      If all_zones is true, 2nd and 3rd for 
      loops set bins for those "extended" interpolations 
      that must cross longitude 180. */
  for( ii=parms.imin; ii < parms.imax; ii++ )
      xx[ii] = parms.xlo + (float)(ii)*parms.dx + parms.dx/2;

  if( all_zones ) {
      /* west of long. 180 */
      for( ii=0; ii < long_extend/2; ii++ )
          xx[ii+parms.imax] = 180 - (long_extend/2 * parms.dx) + (float)(ii)*parms.dx 
                              + parms.dx/2;

      /* east of long. 180; sets center point */
      for( ii=0; ii <= long_extend/2; ii++ )
         xx[ii+parms.imax+long_extend/2] = -180 + (float)(ii)*parms.dx 
                                           + parms.dx/2;
  }

  /* for( ii = 0; ii <=  parms.imax + long_extend; ii++) 
    printf( " ii: %d xx[ii] = %f \n", ii, xx[ii] ); */
 
  
  /* Lat to interpolate to */
  for( jj=parms.jmin; jj < parms.jmax; jj++ )
      yy[jj] = parms.ylo + (float)(jj)*parms.dy + parms.dy/2;

  /* Time to interpolate to */ 
  for( kk=parms.kmin; kk < parms.kmax; kk++ )
      zz[kk] = parms.zlo + (float)(kk)*parms.dz;	

  /* slope (cal) and offset to convert DN to SST */
  cal = 0.15;
  offset = 3.0;

  /* open input data file list */
  if( (fid_infile = fopen( fileio.infile, "r" )) == NULL ){
      printf( "\nCan not open %s\n", fileio.infile );
      exit(1);
  }

  /* position to proper record in file */
  fseek( fid_infile, ( (fileio.istart-1)*RECSIZE ), 0 );



  /* open input aerosol file list if necessary */
  if(aerosol) {
      if( (fid_aerosol_list = fopen( fileio.aerosol_list, "r" )) == NULL ){
          printf( "\nCan not open %s\n", fileio.aerosol_list );
          exit(1);
      }
      /* position to proper record in file */
      fseek( fid_aerosol_list, ( (fileio.istart-1)*RECSIZE ), 0 ); 

  }


  /* read infile one record at a time and process . . . */
  for( k=fileio.istart; k <= fileio.iend; k++ ){

      ipts = 0; 
      strcpy( compress, " " );
      strcpy( tmpstr, "                      " );

      fgets( datafile, sizeof(datafile)+1, fid_infile ); 

      /* strip off newline if necessary */
      if( datafile[ strlen(datafile) ] == '\n' ){ 
          datafile[ strlen(datafile) ] = '\0';
      }
      /* printf("datafile is %s \n", datafile ); */

      /* check to make sure datafile actually exists 
         if not ..... quit program  */
      fid_datafile = fopen( datafile, "r" ); 
      if( fid_datafile == NULL ) {
	  printf( "could not open:\n" );
	  printf( "%s \n", datafile );
	  printf( ". . . quitting program \n" );
	  exit(2);
      }
      fclose( fid_datafile ); 

      basefile = basename( &datafile[0] );
      printf( "\nfile: %s\n", basefile );

 /* e.g, parse year and jul day from 1985004h54dd-gdm.hdf */
      strcpy( tmpstr, basefile ); 
      strncpy( cyr, &tmpstr[0], 4 ); 
      iyr = atoi( cyr );
      strncpy( cday, &tmpstr[4], 3 );
      iday = atoi( cday );
   /* printf(" iday is %d\n", iday ); 
      printf("iyr is %d\n", iyr ); 
   */

 /* if unix compressed, zcat to a temporary filename */
     strncpy( compress, &tmpstr[21], 1 );
     if( strcmp( compress, "Z" ) == 0 ) {
         strcpy( tmpstr, basefile );
         strcpy( tmpfile, "/tmp/" ); 
	 strncat( tmpfile, &tmpstr[0], 20 ); 

	 strcpy( zcatcmd, "zcat ");
	 strcat( zcatcmd, datafile );
	 strcat( zcatcmd, " > " );
	 strcat( zcatcmd, tmpfile );

         (void)  system( zcatcmd );
	 strcpy( datafile,  tmpfile );
     }

     /* do the same for the aerosol file if necessary */
     if( aerosol ) {
      strcpy( compress, " " );
      strcpy( aerosol_tmpstr, "                             " );

      fgets( aerosol_file, sizeof(aerosol_file)+1, fid_aerosol_list ); 
      /* printf("aerosol_file is %s \n", aerosol_file ); */

      /* strip off newline if necessary  */
      if( aerosol_file[ strlen(aerosol_file) ] == '\n' ){ 
          aerosol_file[ strlen(aerosol_file) ] = '\0';
      }


      basefile = basename( &aerosol_file[0] );
      strcpy( aerosol_tmpstr, basefile ); 
      printf( "aerosol file: %s\n", basefile ); 


     /* if unix compressed, zcat to a temporary filename  */
     strncpy( compress, &aerosol_tmpstr[27], 1 );
     if( strcmp( compress, "Z" ) == 0 ) {
         strcpy( aerosol_tmpstr, basefile );
         strcpy( aerosol_tmpfile, "/tmp/" ); 
	 strncat( aerosol_tmpfile, &aerosol_tmpstr[0], 26 ); 

	 strcpy( zcatcmd, "zcat ");
	 strcat( zcatcmd, aerosol_file );
	 strcat( zcatcmd, " > " );
	 strcat( zcatcmd, aerosol_tmpfile );

         (void)  system( zcatcmd );
	 strcpy( aerosol_file,  aerosol_tmpfile );
     }
     }

         /* read the aerosol file into an array */
	 if( aerosol ) { 
	     fid_aerosol_file = fopen( aerosol_file, "r" );
             fread( aerosol_corr, sizeof(uint8), parms.y_length * parms.x_length, fid_aerosol_file );
	     fclose( fid_aerosol_file );
	 }

         /* read HDF SST data dimensions */
	 retn = DFR8getdims( datafile, &width, &height, &haspal );
	 /* printf( "HDF width and height: %d %d \n", width, height ); */

	 if( width != parms.x_length || height != parms.y_length ) {
	     printf( "\nbounds mismatch after reading HDF file dimensions !!\n" );
	     printf( "width: %d, parms.x_length: %d \n", width, parms.x_length ); 
	     printf( "height: %d, parms.y_length: %d \n", height, parms.y_length ); 
	     printf( "skipping %s . . . \n\n", datafile );
	     continue;
         }

 /* ------------------------------------------------- */
 /* Read the SST image of the HDF file.
    HDF call: DFR8getimage()  */
 /* ------------------------------------------------- */

	 retn=DFR8getimage(datafile, (VOIDP)in_data, parms.x_length, 
			   parms.y_length, NULL);

 /* ------------------------------------------------- */
 /* If "all pixel" data choosen read the quality  
    flag image */
 /* ------------------------------------------------- */
	
	if( sst_type )  
	   retn=DFR8getimage(datafile, (VOIDP)quality_data, 
			   parms.x_length, parms.y_length, NULL);



 /* ------------------------------------------------- */
 /* Read hdf file DN values and convert to SST.
    If "all pixel" data read only those pixels 
    with a quality flag greater than or equal to 
    quality_flag */
 /* ------------------------------------------------- */

        for( i=hdf.icolLeft; i <= hdf.icolRight; i++ ) {
	  for( j=hdf.irowUpper; j <= hdf.irowLower; j++ ) {

              /* only conider aerosol correction greater than 5 cnt */
	      if( aerosol && aerosol_corr[j][i] > 5  && in_data[j][i] > 0 )
		  in_data[j][i] = in_data[j][i] + aerosol_corr[j][i];

	      /* perform Casey cloud erosion on non-zero SST data */
	      /* if nearest neighbor is a cloud, set pixel to cloud (0) */
	      if( in_data[j][i] > 0 && cloud_erosion ) {
                  for( col = i-1; col <= i+1; col++ ) {
                     for( row = j-1; row <= j+1; row++ ) { 
	             /* make sure we are searching within 
		     image array bounds */
	              if( col >= 0 && col < parms.x_length &&  
		          row >= 0 && row < parms.y_length && 
		          in_data[row][col] == 0 ) {
		            in_data[j][i] = 0;
			    goto end_erosion;
                      }
                    }
                  }
	      }
	      end_erosion:


              /* if all_pixel (sst_type = 1) filter for quality level, 
		 or if best_sst accept data values greater than 0  */
		  
		  /*printf( "quality flag is %d \n", quality_flag);
		  printf (" sst_type is %d \n", sst_type);
		  */

	      if( (sst_type && quality_data[j][i] >= quality_flag) || 
		  (!sst_type && in_data[j][i] > 0) ) {
		  /* printf (" quality_data = %d  \n", quality_data[j][i]); */

                  f[ipts] = cal * (float)in_data[j][i]  - offset;
                  x[ipts] = -180. + parms.dxd * (float)(i) + parms.dxd/2;
                  y[ipts] = 90. - parms.dyd * (float)(j) - parms.dyd/2;
                  z[ipts] = (float)( iday+( (iyr-1985)*365 ) );

                  if(iyr > 1988) z[ipts]=z[ipts]+1;
                  if(iyr > 1992) z[ipts]=z[ipts]+1;
                  if(iyr > 1996) z[ipts]=z[ipts]+1;
                  if(iyr > 2000) z[ipts]=z[ipts]+1;
                  if(iyr > 2004) z[ipts]=z[ipts]+1;
                  if(iyr > 2008) z[ipts]=z[ipts]+1;
		  ipts++;
	      }
	  }
        } 

 /* calculate interpolation weights and vsum  */

	  printf(" num of pts in file: %d \n", ipts );
	  binsum( ipts );

        if( strcmp( compress, "Z" ) == 0 ) {
	    strcpy( rmcmd, "rm -f " );
	    strcat( rmcmd, tmpfile );
	    system( rmcmd ); 
	    if( aerosol ) {
	        strcpy( rmcmd, "rm -f " );
	        strcat( rmcmd, aerosol_tmpfile );
	        system( rmcmd ); 
            }
        }
 /* ..... read next hdf file from infile */

  }
  fclose( fid_infile );
  if( aerosol )
      fclose( fid_aerosol_list );

 /* all input files processed; calculate interpolated SST maps */
      for( kk=0; kk < parms.kmax; kk++ ) {
        for( jj=0; jj < parms.jmax; jj++ ) {
          for( ii=0; ii < parms.imax; ii++ ) {
            if ( vsum[1][ii][jj][kk] > 0 ) {  

		/* reverse jj indicies to flip N to S */
		jj_reverse = parms.jmax - jj - 1;
                sl[ii][jj_reverse][kk] = 
		vsum[0][ii][jj][kk] / vsum[1][ii][jj][kk];
            }
          }
        }
      }

 /* write output as map "interleaved" or map "sequential".
    both formats preceded by header.
    see gaussinterp.readme for more info.
 */

 /* geo-positions of UL and LR corners */
      ULlon = parms.xlo + parms.dx/2;
      ULlat = parms.yhi - parms.dy/2;
      LRlon = parms.xhi - parms.dx/2;
      LRlat = parms.ylo + parms.dy/2;

 /* version number, "c" refers to C version */
      strcpy( version, "c1.3" );
      strcpy( plabel, "sst interpolation");

 /*  write the header */
      if( strcmp( fileio.outformat, "formatted" ) == 0 ) { 
	  fprintf( fid_outfile, "%s -- %s\n ", version, plabel );
	  fprintf( fid_outfile, "%d %d %d\n", 
		   parms.imax,parms.jmax,parms.kmax );
	  fprintf( fid_outfile, "%f %f %f\n", 
		   parms.xwin2,parms.ywin2,parms.zwin2 );
	  fprintf( fid_outfile, "%f %f %f\n", 
		   parms.xh,parms.yh,parms.zh );
	  fprintf( fid_outfile, "%f %f %f\n", 
		   parms.dx,parms.dy,parms.dz );
	  fprintf( fid_outfile, "%f %f %f %f\n", 
	  	   ULlon,ULlat,LRlon,LRlat );
          fprintf( fid_outfile, "%ld %ld\n",
                   date.startdate, date.enddate );
      }
      else if( strcmp( fileio.outformat,  "unformatted" ) == 0 ) { 
	  magic_num = MAGIC;
	  fwrite( &magic_num, sizeof(magic_num),1,fid_outfile ); 
	  fwrite( &parms.imax, sizeof(parms.imax),1,fid_outfile );
	  fwrite( &parms.jmax, sizeof(parms.jmax),1,fid_outfile );
	  fwrite( &parms.kmax, sizeof(parms.kmax),1,fid_outfile );
	  fwrite( &parms.xwin2, sizeof(parms.xwin2),1,fid_outfile );
	  fwrite( &parms.ywin2, sizeof(parms.ywin2),1,fid_outfile );
	  fwrite( &parms.zwin2, sizeof(parms.zwin2),1,fid_outfile );
	  fwrite( &parms.xh, sizeof(parms.xh),1,fid_outfile );
	  fwrite( &parms.yh, sizeof(parms.yh),1,fid_outfile );
	  fwrite( &parms.zh, sizeof(parms.zh),1,fid_outfile );
	  fwrite( &parms.dx, sizeof(parms.dx),1,fid_outfile );
	  fwrite( &parms.dy, sizeof(parms.dy),1,fid_outfile );
	  fwrite( &parms.dz, sizeof(parms.dz),1,fid_outfile );
	  fwrite( &ULlon, sizeof(ULlon),1,fid_outfile );
	  fwrite( &ULlat, sizeof(ULlat),1,fid_outfile );
	  fwrite( &LRlon, sizeof(LRlon),1,fid_outfile );
	  fwrite( &LRlat, sizeof(LRlat),1,fid_outfile );
          fwrite( &date.startdate, sizeof(date.startdate),1,fid_outfile);
	  fwrite( &date.enddate, sizeof(date.enddate),1,fid_outfile);
	  /* binary header is 76 bytes */
      }

 /*  write the maps */
      if( strcmp( fileio.mapformat, "interleave" ) == 0 ) {
        printf( "\n map output is interleave\n" );
        for( jj=0; jj < parms.jmax; jj++ ) {
          for( ii=0; ii < parms.imax; ii++ ) {
            for( kk=0; kk < parms.kmax; kk++ ) {
                sst_inter[kk] = sl[ii][jj][kk]; 

		/* scale sst back to 0-255 DNs */
		if( sst_inter[kk] != 0 ) 
		    sstDN = (char) nint( (sst_inter[kk] + offset) / cal );
                else
		    sstDN = 0;	/* O's in array sst_inter are missing data */ 

	        if( strcmp( fileio.outformat, "formatted" ) == 0 ) 
		    fprintf( fid_outfile, "%d ", sstDN ); 
                else 
		    fwrite( &sstDN, sizeof(char),1,fid_outfile ); 

             }
          }
        }
      } else if( strcmp( fileio.mapformat, "sequential" ) == 0 ) {
        printf( "\n map output is sequential\n" );
        for( kk=0; kk < parms.kmax; kk++ ) {
          for( jj=0; jj < parms.jmax; jj++ ) {
            for( ii=0; ii < parms.imax; ii++ ) {
               sst_seq[ii] = sl[ii][jj][kk]; 

		/* scale sst back to 0-255 DNs */
	       if( sst_seq[ii] != 0 ) 
	           sstDN = (char) nint( (sst_seq[ii] + offset) / cal );
               else
		   sstDN = 0;	/* O's in array sst_seq are missing data */ 

               if( strcmp( fileio.outformat, "formatted" ) == 0 ) 
	           fprintf( fid_outfile, "%d ", sstDN ); 
               else 
	     	   fwrite( &sstDN, sizeof(char),1,fid_outfile );

	    }
          }
        }
      }
  fclose( fid_outfile );
}
