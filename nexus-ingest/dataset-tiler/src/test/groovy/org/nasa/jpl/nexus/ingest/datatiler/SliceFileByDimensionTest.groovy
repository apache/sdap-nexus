
package org.nasa.jpl.nexus.ingest.datatiler

import org.junit.Test

import static junit.framework.Assert.assertEquals
import static org.hamcrest.CoreMatchers.hasItems
import static org.hamcrest.MatcherAssert.assertThat

/**
 * Created by greguska on 4/18/16.
 */
class SliceFileByDimensionTest {

    @Test
    public void testGenerateTileBoundrySlices(){

        def slicer = new SliceFileByDimension()
        slicer.setSliceByDimension("NUMROWS")

        def dimensionNameToLength = ['NUMROWS':3163, 'NUMCELLS':82]

        def result = slicer.generateTileBoundrySlices("NUMROWS", dimensionNameToLength)

        assertEquals(3163, result.size)

        assertThat(result, hasItems("NUMROWS:0:1,NUMCELLS:0:82", "NUMROWS:1:2,NUMCELLS:0:82", "NUMROWS:3162:3163,NUMCELLS:0:82"))

    }

    @Test
    public void testGenerateTileBoundrySlices2(){

        def slicer = new SliceFileByDimension()
        slicer.setSliceByDimension("NUMROWS")

        def dimensionNameToLength = ['NUMROWS':2, 'NUMCELLS':82]

        def result = slicer.generateTileBoundrySlices("NUMROWS", dimensionNameToLength)

        assertEquals(2, result.size)

        assertThat(result, hasItems("NUMROWS:0:1,NUMCELLS:0:82", "NUMROWS:1:2,NUMCELLS:0:82"))

    }
}
