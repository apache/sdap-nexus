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
