/* $Revision:$ */
/* Apply the Casey cloud erosion to the satellite data.
   This algorithm flags as cloudy all data with one 
   nearest neighbor of a Pathfinder flagged cloud pixel. */

#include "interp.h"
#include "hdf.h"

clouderosion( float in_data, int i, int j ) {
    int row, col;

    /* for each image pixel loop over
       nearest neighbors; if any neightbor is
       a cloud, set temp[j][i] to cloud value (0)
    */
    for( col = i-1; col <= i+1; col++ ) {
        for( row = j-1; row <= j+1; row++ ) {
	    printf( "col is %d, row is %d\n", col, row );
	    /* make sure we are searching within image array bounds */
	    if( col >= 0 && col < parms.x_length &&  
		row >= 0 && row < parms.y_length ) {
	    printf( "valid col is %d, valid row is %d\n\n", col, row );
		if( in_data[col][row] == 0 ) {
		    temp[j][i] = 0.;
		    printf( "in_data[%d][%d] is %c\n", col, row, in_data[col][row] );
		    printf( " setting temp[%d][%d] to zero\n\n\n", j,i );
		    break;
                }
            }
         }
    }

}
