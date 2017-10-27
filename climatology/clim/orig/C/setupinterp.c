/* NAME gaussinterp (C version)
   16 Jan 00    earmstrong      NASA/JPL
   $Revision: 1.16 $

  DESCRIPTION
  Gaussian interpolation program modified to map SST (HDF) data.
  This program is a C port of the fortran version of gaussinterp.

  SYNOPSIS
  setupinterp() allocates space for arrays, calls interp()
  which does the interpolation

  USAGE
  % gaussinterp <infile> <beg_rec> <parmfile> <outfile> */


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libgen.h>
#include <math.h>
#include "interp.h"
/* see interp.h for global structs and arrays */
      
main( int argc, char *argv[] ) {

      int iday, iyr, calloc_bytes, rec_step;
      long max_infile_size;

      int value;
      FILE *fid_infile, *fid_parm;

      char datafile[RECSIZE];
      char *basefile;
      char *parmfile;
      char *startrec, *endrec;
      char cday[3], cyr[4], cdate[7];
      char *cstartdate;
      char *cenddate;
      char tmpline[80];

/* check command line arguments */
      if( argc != 5 && argc != 6 ) {
	  usage();
	  exit(1);
      }
      fileio.infile = argv[1];
      startrec = argv[2];
      parmfile = argv[3];
      fileio.outfile = argv[4];
      if( argc == 6 )
          fileio.aerosol_list = argv[5];  

      /* open parameter file and read line by line */
      if( (fid_parm = fopen( parmfile, "r" )) == NULL ){
	  printf( "\nCan not open %s\n", parmfile ); 
	  exit(1);
      }

      fgets( tmpline, sizeof(tmpline), fid_parm );
      sscanf( tmpline, "%d", &parms.ndatamax ); 
      fgets( tmpline, sizeof(tmpline), fid_parm );
      sscanf( tmpline, "%d %d", &parms.x_length, &parms.y_length ); 
      fgets( tmpline, sizeof(tmpline), fid_parm );
      sscanf( tmpline, "%d", &parms.kmax ); 
      fgets( tmpline, sizeof(tmpline), fid_parm );
      sscanf( tmpline, "%f %f %f", &parms.xwin2, &parms.ywin2, &parms.zwin2 ); 
      fgets( tmpline, sizeof(tmpline), fid_parm );
      sscanf( tmpline, "%f %f %f", &parms.xh, &parms.yh, &parms.zh ); 
      fgets( tmpline, sizeof(tmpline), fid_parm );
      sscanf( tmpline, "%f %f %f", &parms.dx, &parms.dy, &parms.dz ); 
      fgets( tmpline, sizeof(tmpline), fid_parm );
      sscanf( tmpline, "%f %f", &parms.xlo, &parms.xhi ); 
      fgets( tmpline, sizeof(tmpline), fid_parm );
      sscanf( tmpline, "%f %f", &parms.ylo, &parms.yhi ); 
      fgets( fileio.mapformat, sizeof(fileio.mapformat), fid_parm );
      fgets( fileio.outformat, sizeof(fileio.outformat), fid_parm );
      fgets( tmpline, sizeof(tmpline), fid_parm );
      sscanf( tmpline, "%d", &cloud_erosion );
      fgets( tmpline, sizeof(tmpline), fid_parm );
      sscanf( tmpline, "%d", &sst_type );
      fgets( tmpline, sizeof(tmpline), fid_parm );
      sscanf( tmpline, "%d", &quality_flag );
      fgets( tmpline, sizeof(tmpline), fid_parm );
      sscanf( tmpline, "%d", &aerosol );

      fileio.mapformat[ strlen(fileio.mapformat)-1 ] = '\0';
      fileio.outformat[ strlen(fileio.outformat)-1 ] = '\0';

      fclose( fid_parm );
      
      if( strcmp(fileio.mapformat, "interleave") != 0  &&   
          strcmp(fileio.mapformat, "sequential") != 0 ) {
	   printf( "check parameter file for interleave or sequential maps !\n" );
	   exit(1);
      } 
      if( strcmp(fileio.outformat, "formatted") != 0  &&   
          strcmp(fileio.outformat, "unformatted") != 0 ) {
	   printf( "check parameter file for formatted or unformatted output !\n" );
	   exit(1);
      }

    /* Calculate number of interpolation "steps" based on
       long and lat ranges divided by interpolation interval.
       Origin is at lon=-180, lat=90 */
      parms.imax = floorf((parms.xhi + 180) / parms.dx) - floorf((parms.xlo + 180) / parms.dx );
      parms.jmax = floorf((parms.yhi - 90) / parms.dy) - floorf((parms.ylo - 90) / parms.dy );
      printf( "imax, jmax, kmax: %d %d %d \n", 
		parms.imax, parms.jmax, parms.kmax );

      parms.imin = 0;
      parms.jmin = 0;
      parms.kmin = 0;

    /* calculate degrees per grid point */
      parms.dxd =  360. / (float)parms.x_length;
      parms.dyd =  180. / (float)parms.y_length;

    /* find columns and rows for subsetting into hdf image array 
       remember @ hdfarray(0,0) -->  lon=-180, lat=90 */
      hdf.icolLeft  = floorf( (parms.xlo + 180.) / parms.dxd );
      hdf.icolRight = floorf( ((parms.xhi + 180.) / parms.dxd) - 1. );
      hdf.irowUpper = floorf( fabsf(parms.yhi - 90.) / parms.dyd );
      hdf.irowLower = floorf( (fabsf(parms.ylo - 90.) / parms.dyd) - 1. );

      /* printf( "array corners: %d %d %d %d \n", hdf.icolLeft, hdf.irowUpper, 
		  hdf.icolRight ,hdf.irowLower); */

   /* set global interpolation flag if necessary */
      if( parms.xlo == -180. && parms.xhi == 180. ) {
          all_zones = 1;  /* true */
          printf( "all_zones is true: interpolating across -180 West\n" );
      }

      if( cloud_erosion )
	  printf( "performing cloud erosion filtering on SST data \n" );
      if( sst_type )
	  printf( "working with 'all pixel' data: quality flag is %d \n", quality_flag );
      if( aerosol )
	  printf( "performing aerosol correction \n" );

   /* allocate space for arrays  */
      calloc_bytes = 0;
      if( (temp = fmal2d(  parms.x_length, parms.y_length )) == NULL ) 
	  printf( "not enough memory for array temp\n" ), exit(-1);
      else calloc_bytes = parms.x_length * parms.y_length * sizeof(float);  

      if( (in_data = cmal2d( parms.x_length, parms.y_length )) == NULL ) 
	  printf( "not enough memory for array in_data\n" ), exit(-1);
      else calloc_bytes += parms.x_length * parms.y_length * sizeof(char);  

      if( (quality_data = cmal2d( parms.x_length, parms.y_length )) == NULL ) 
	  printf( "not enough memory for array quality_data\n" ), exit(-1);
      else calloc_bytes += parms.x_length * parms.y_length * sizeof(char);  

       if( (sl = fmal3d( parms.imax, parms.jmax, parms.kmax )) == NULL )
	  printf( "not enough memory for array sl\n" ), exit(-1);
      else calloc_bytes += parms.imax * parms.jmax * parms.kmax * sizeof(float);  

      if( (vsum = fmal4d( 2, parms.imax, parms.jmax, parms.kmax )) == NULL )
	  printf( "not enough memory for array vsum\n" ), exit(-1);
      else calloc_bytes += parms.imax * parms.jmax * parms.kmax * 2 * sizeof(float);  
    
      if( (sst_seq = (float *)calloc( parms.imax, sizeof(float) )) == NULL ) 
	  printf( "not enough memory for array sst_seq\n" ), exit(-1);
      else calloc_bytes += parms.imax * sizeof(float);  

      if( (sst_inter = (float *)calloc( parms.kmax, sizeof(float) )) == NULL )
	  printf( "not enough memory for array sst_inter\n" ), exit(-1);
      else calloc_bytes += parms.kmax * sizeof(float);  

      if( (x = (float *)calloc( parms.ndatamax, sizeof(float) )) == NULL ) 
	  printf( "not enough memory for array x\n" ), exit(-1);
      else calloc_bytes += parms.ndatamax * sizeof(float);  

      if( (y = (float *)calloc( parms.ndatamax, sizeof(float) )) == NULL )
	  printf( "not enough memory for array y\n" ), exit(-1);
      else calloc_bytes += parms.ndatamax * sizeof(float);  

      if( (z = (float *)calloc( parms.ndatamax, sizeof(float) )) == NULL )
	  printf( "not enough memory for array z\n" ), exit(-1);
      else calloc_bytes += parms.ndatamax * sizeof(float);  

      if( (f = (float *)calloc( parms.ndatamax, sizeof(float) )) == NULL )
	  printf( "not enough memory for array f\n" ), exit(-1);
      else calloc_bytes += parms.ndatamax * sizeof(float);  

      /* Extend array xx by long_extend to handle crossing
	 longitude 180 during global interpolation.
         Extend by 4*search radius (plus 1 in xx dimension
         for -180 West crossing point */
      if( all_zones ) {
          long_extend = nint( 4 * parms.xwin2 / parms.dx );
          /* make sure long_extend is even for equal number of
	     bins on either side of -180 W  */
          if( long_extend % 2 != 0 )
              long_extend++;
          if( (xx = (float *)calloc( parms.imax + long_extend + 1, sizeof(float) )) == NULL )
	      printf( "not enough memory for array xx\n" ), exit(-1);
          else calloc_bytes += (parms.imax + long_extend + 1) * sizeof(float);
      } else {
             if( (xx = (float *)calloc( parms.imax, sizeof(float) )) == NULL )
	         printf( "not enough memory for array xx\n" ), exit(-1);
             else calloc_bytes += parms.imax * sizeof(float);
      }  
 
      if( (yy = (float *)calloc( parms.jmax, sizeof(float) )) == NULL)
	  printf( "not enough memory for array yy\n" ), exit(-1);
      else calloc_bytes += parms.jmax * sizeof(float);  

      if( (zz = (float *)calloc( parms.kmax, sizeof(float) )) == NULL )
	  printf( "not enough memory for array zz\n" ), exit(-1);
      else calloc_bytes += parms.kmax * sizeof(float);  

      if( (ixmin = (int *)calloc( parms.ndatamax, sizeof(int) )) == NULL ) 
	  printf( "not enough memory for array ixmin\n" ), exit(-1);
      else calloc_bytes += parms.ndatamax * sizeof(int);  

      if( (ixmax = (int *)calloc( parms.ndatamax, sizeof(int) )) == NULL ) 
	  printf( "not enough memory for array ixmax\n" ), exit(-1);
      else calloc_bytes += parms.ndatamax * sizeof(int);  

      if( (iymin = (int *)calloc( parms.ndatamax, sizeof(int) )) == NULL ) 
	  printf( "not enough memory for array iymin\n" ), exit(-1);
      else calloc_bytes += parms.ndatamax * sizeof(int);  

      if( (iymax = (int *)calloc( parms.ndatamax, sizeof(int) )) == NULL ) 
	  printf( "not enough memory for array iymax\n" ), exit(-1);
      else calloc_bytes += parms.ndatamax * sizeof(int);  

      if( (izmin = (int *)calloc( parms.ndatamax, sizeof(int) )) == NULL ) 
	  printf( "not enough memory for array izmin\n" ), exit(-1);
      else calloc_bytes += parms.ndatamax * sizeof(int);  

      if( (izmax = (int *)calloc( parms.ndatamax, sizeof(int) )) == NULL ) 
	  printf( "not enough memory for array izmax\n" ), exit(-1);
      else calloc_bytes += parms.ndatamax * sizeof(int);  


      printf( "done calloc arrays\n" );  
      printf( "total bytes used: %d (%5.1f Mb)\n\n", calloc_bytes, calloc_bytes/1e6 ); 


      /* Determine zlo which corresponds to record number in infile for
	 first map center and number of days since 1 Jan 1985 */
      fileio.istart = atoi( startrec );
 
      /* Open input file list and read first record */
      if( (fid_infile = fopen( fileio.infile, "r" )) == NULL ){
	  printf( "\nCan not open %s\n", fileio.infile ); 
	  exit(1);
      }

      /* position to proper record in file */
      fseek( fid_infile, (fileio.istart-1)*RECSIZE, SEEK_SET );
      fgets( datafile, sizeof(datafile), fid_infile );
      fclose( fid_infile );
      basefile = basename( &datafile[0] );

      strcpy( cday, "   " );  /* initialize strings */
      strcpy( cyr, "    " ); 
      /* e.g, parse year and jul day from 1985004h54dd-gdm.hdf */
      strncpy( cyr, &basefile[0], 4 );
      iyr = atoi( cyr );
      strncpy( cday, &basefile[4], 3 );
      iday = atoi( cday );

      /* set parms.zlo: days since 1 Jan 1985 */
      parms.zlo = (float)iday+(float)(iyr-1985)*365.;
      if(iyr > 1988) parms.zlo = parms.zlo + 1.;
      if(iyr > 1992) parms.zlo = parms.zlo + 1.;
      if(iyr > 1996) parms.zlo = parms.zlo + 1.;
      if(iyr > 2000) parms.zlo = parms.zlo + 1.;
      if(iyr > 2004) parms.zlo = parms.zlo + 1.;
      if(iyr > 2008) parms.zlo = parms.zlo + 1.;
      printf( " zlo is  %f\n", parms.zlo );
      printf( " first map centered on day: %d in year: %d\n", iday, iyr );

      /* Set the first center dates */
      cstartdate = strcat( cdate, cyr );
      cstartdate = strcat( cdate, cday );
      date.startdate = atoi( cstartdate );
      printf( " start date is %d \n", date.startdate );

      /* reopen file to determine last map center date */
      fopen( fileio.infile, "r" );
      fileio.iend = fileio.istart + (parms.kmax-1) * parms.dz;
      fseek(fid_infile, (fileio.iend-1)*RECSIZE, SEEK_SET );
      fgets( datafile, sizeof(datafile), fid_infile );
      fclose( fid_infile );
      basefile = basename( &datafile[0] );
  
      /* parse year and jul day  */
      strcpy( cdate, "" );
      strncpy( cyr, &basefile[0], 4 );
      strncpy( cday, &basefile[4], 3 );
      cenddate = strcat( cdate, cyr );
      cenddate = strcat( cdate, cday );
      date.enddate = atoi( cenddate );
      printf( " end date is %d\n", date.enddate );

      /* Now determine temporal interpolation (record) ranges */
      /* set istart and iend so the first and last maps will span a complete
	 temporal interpolation range if possible. e.g., if first map is centered
	 on 1 Jan 1986 (this is date of first record to read from the 
	 input datafile [fileio.istart]), with a search radius of 5 days (zwin2),
         set istart back five days to 27 Dec 1985 and iend to 6 Jan 1986 */

      /* set initial step back and forward size to temporal search radius */ 
      rec_step = (int) parms.zwin2;  
            
      /* reopen input datafile */
      if( (fid_infile = fopen( fileio.infile, "r" )) == NULL ){
	  printf( "\nCan not open %s\n", fileio.infile ); 
	  exit(1);
      }

      /* determine max bytes in infile */  
      fseek( fid_infile, 0L, SEEK_END );
      max_infile_size = ftell( fid_infile );

      /* step back into infile; use return of fseek() to determine if 
         file pointer is set before beginning of file  */
      while( fseek(fid_infile, (fileio.istart-1-rec_step)*RECSIZE, SEEK_SET) != 0 ) 
          rec_step--;
      fileio.istart -= rec_step;

      /* step forward into infile */
      rec_step = parms.zwin2; 
      fseek( fid_infile, (fileio.iend-1+rec_step)*RECSIZE, SEEK_SET );

      /* if the file pointer is set beyond the EOF: step back */
      while( ftell(fid_infile) >= max_infile_size ) {
        rec_step--;  
	fseek( fid_infile, (fileio.iend-1+rec_step)*RECSIZE, SEEK_SET );
      }
      fileio.iend += rec_step;
 
      printf( " records: istart %d and iend %d \n\n", fileio.istart, fileio.iend );
      fclose( fid_infile );           
    
      /* call interp() */
      interp();
      
      free(temp), free(sl), free(vsum), free(sst_seq), free(sst_inter);
      free(x), free(y), free(z), free(f), free(xx), free(yy), free(zz);
      free(ixmin), free(ixmax), free(iymin), free(iymax);
      free(izmin), free(izmax), free(in_data), free(quality_data);
 }

/* functions to allocate space for float arrays */
/* 2d dimension */
float **fmal2d( int ix, int iy ){

 float **ival;
 int i;

 ival = (float **)calloc( ix, sizeof(float *) );
 for( i=0; i < ix; i++ )
     ival[i] = (float *)calloc( iy, sizeof(float) );

 return(ival);
}

 /* 3d dimension */
 float ***fmal3d( int ix, int iy, int iz ){

 float ***ival;
 int i, j;

 ival = (float ***)calloc( ix, sizeof(float **) );
 for( i=0; i < ix; i++ )
     ival[i] = (float **)calloc( iy, sizeof(float *) );

 for( i=0; i < ix; i++ )
     for( j=0; j < iy; j++ )
         ival[i][j] = (float *)calloc( iz, sizeof(float) );
 
 return(ival);
 }

/* 4d dimension */
float ****fmal4d( int ix, int iy, int iz, int ia ){
     
    float ****ival;
    int i, j, k;

    ival = (float ****)calloc( ix, sizeof(float ***) );
    for( i=0; i < ix; i++ )
	ival[i] = (float ***)calloc( iy, sizeof(float **) );
    for( i=0; i < ix; i++ )
	for( j=0; j < iy; j++)
	    ival[i][j] = (float **)calloc( iz, sizeof(float *) );   
    for( i=0; i < ix; i++ )
	for( j=0; j < iy; j++)
	    for( k=0; k < iz; k++)
	        ival[i][j][k] = (float *)calloc( ia, sizeof(float) );   

    return(ival);
}

/* function to allocate space for char array */
/* 2d dimension */
char **cmal2d( int ix, int iy ) {

 char **ival;
 int i;

 ival = (char **)calloc( ix, sizeof(char *) );
 for( i=0; i < ix; i++ )
     ival[i] = (char *)calloc( iy, sizeof(char) );

 return(ival);
}



usage() {
	  printf( "	~~ gaussian interpolation of pathfinder SST data (C version) ~~\n\n " );
	  printf(  "USAGE:  gaussinterp <infile> <beg_rec> <parmfile> <outfile>\n" );
	  printf(  " -- infile: file containing list to process\n" );
	  printf(  " -- beg_rec: first record (file) in infile processed\n" );
	  printf(  " -- parmfile: input parameter file (e.g., interp.parms)\n" );
	  printf(  " -- outfile: interpolated output file (ascii or binary)\n" );
	  printf(  "\n e.g.,  gaussinterp list.dat 100 interp.parms output.dat\n" );
	  printf(  "[process records of list.dat starting from record 100; interpolated \
result is output.dat]\n" );
	  printf(  "note: see interp.parms for example format of parameter file\n" );
	  printf(  "      see gaussinterp.readme for more general information\n" );
	  return(0);
}
