
package org.nasa.jpl.nexus.ingest.datatiler

import org.junit.Test

import static junit.framework.Assert.assertEquals
import static org.hamcrest.CoreMatchers.hasItems
import static org.hamcrest.MatcherAssert.assertThat

/**
 * Created by greguska on 4/18/16.
 */
class SliceFileByTilesDesiredTest {

    @Test
    public void testGenerateChunkBoundrySlices(){

        def slicer = new SliceFileByTilesDesired()

        def chunksDesired = 5184
        def dimensionNameToLength = ['lat':17999, 'lon':36000]

        def result = slicer.generateChunkBoundrySlices(5184, dimensionNameToLength)

        assertEquals(chunksDesired + 72, result.size)

        assertThat(result, hasItems("lat:0:249,lon:0:500", "lat:0:249,lon:500:1000", "lat:17928:17999,lon:35500:36000"))

    }
}
