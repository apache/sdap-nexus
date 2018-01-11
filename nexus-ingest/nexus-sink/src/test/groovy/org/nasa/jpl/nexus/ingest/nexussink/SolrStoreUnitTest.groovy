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

package org.nasa.jpl.nexus.ingest.nexussink

import org.junit.Test
import org.nasa.jpl.nexus.ingest.wiretypes.NexusContent
import org.springframework.mock.env.MockEnvironment

import static org.junit.Assert.assertEquals

/**
 * Created by greguska on 5/2/17.
 */
class SolrStoreUnitTest {

    @Test
    public void testGetSolrDocFromTileSummary() {
        def solrStore = new SolrStore(null);
        solrStore.setEnvironment(new MockEnvironment())

        def tileSummary = NexusContent.TileSummary.newBuilder()
                .setTileId("1")
                .setBbox(NexusContent.TileSummary.BBox.newBuilder()
                .setLatMin(51)
                .setLatMax(55)
                .setLonMin(22)
                .setLonMax(30)
                .build())
                .setDatasetName("test")
                .setDatasetUuid("4")
                .setDataVarName("sst")
                .setGranule("test.nc")
                .setSectionSpec("0:1,0:1")
                .setStats(NexusContent.TileSummary.DataStats.newBuilder()
                .setCount(10)
                .setMax(50)
                .setMin(50)
                .setMean(50)
                .setMaxTime(1429142399)
                .setMinTime(1429142399)
                .build())
                .build()

        def doc = solrStore.getSolrDocFromTileSummary(tileSummary)

        assertEquals("2015-04-15T23:59:59Z", doc.get("tile_min_time_dt").value)
        assertEquals("2015-04-15T23:59:59Z", doc.get("tile_max_time_dt").value)
        assertEquals("sea_surface_temp", doc.get('table_s').value)
        assertEquals("POLYGON((22.0 51.0, 30.0 51.0, 30.0 55.0, 22.0 55.0, 22.0 51.0))", doc.get('geo').value)
        assertEquals("1", doc.get('id').value)
        assertEquals("4", doc.get('dataset_id_s').value)
        assertEquals("0:1,0:1", doc.get('sectionSpec_s').value)
        assertEquals("test", doc.get('dataset_s').value)
        assertEquals("test.nc", doc.get('granule_s').value)
        assertEquals("sst", doc.get('tile_var_name_s').value)
        assertEquals(22.0f, (Float) doc.get('tile_min_lon').value, 0.01f)
        assertEquals(30.0f, (Float) doc.get('tile_max_lon').value, 0.01f)
        assertEquals(51.0f, (Float) doc.get('tile_min_lat').value, 0.01f)
        assertEquals(55.0f, (Float) doc.get('tile_max_lat').value, 0.01f)
        assertEquals(50.0f, (Float) doc.get('tile_min_val_d').value, 0.01f)
        assertEquals(50.0f, (Float) doc.get('tile_max_val_d').value, 0.01f)
        assertEquals(50.0f, (Float) doc.get('tile_avg_val_d').value, 0.01f)
        assertEquals(10, doc.get('tile_count_i').value)
        assertEquals("test!1", doc.get('solr_id_s').value)
    }
}
