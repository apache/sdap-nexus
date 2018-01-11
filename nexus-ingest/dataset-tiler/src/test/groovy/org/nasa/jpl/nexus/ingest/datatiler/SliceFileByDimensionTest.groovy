/* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

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
