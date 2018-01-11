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

import ucar.nc2.dataset.NetcdfDataset
import ucar.nc2.dt.grid.GeoGrid
import ucar.nc2.ft.FeatureDatasetFactoryManager

/**
 * Created by greguska on 2/1/16.
 */
class SliceFileByTilesDesired implements FileSlicer{

    private Integer tilesDesired
    private List<String> dimensions

    public void setTilesDesired(Integer desired) {
        this.tilesDesired = desired
    }

    public void setDimensions(List<String> dims) {
        this.dimensions = dims
    }

    def generateSlices(def inputfile) {

        NetcdfDataset ds = null
        def dimensionNameToLength = [:]
        try {
            ds = NetcdfDataset.openDataset(inputfile.getAbsolutePath())

            dimensionNameToLength = ds.getDimensions().findResults { dimension ->

                this.dimensions.contains(dimension.getShortName()) ? [(dimension.getShortName()): dimension.getLength()] : null

            }.collectEntries { it }
        } finally {
            ds?.close()
        }

        return generateChunkBoundrySlices(tilesDesired, dimensionNameToLength)

    }

    def generateChunkBoundrySlices(def chunksDesired, def dimensionNameToLength) {
        def combos = dimensionNameToLength.collect { dimensionName, lengthOfDimension ->
            [dimensionName, lengthOfDimension, calculateStepSize(lengthOfDimension, chunksDesired, dimensionNameToLength.size())]
        }.collect { nameLengthSize ->
            def dimname = nameLengthSize[0]
            def length = nameLengthSize[1]
            def step = nameLengthSize[2]
            def bounds = []
            0.step(length, step) { start ->
                bounds.add("$dimname:$start:${start + step >= length ? length : start + step}")
            }
            bounds
        }.combinations().collect { combo ->
            combo.join(",")
        }

        return combos

    }

    def calculateStepSize(def lengthOfDimension, def chunksDesired, def numberOfDimensions) {
        return Math.floor(lengthOfDimension / (chunksDesired**(1.0 / numberOfDimensions))).toInteger()
    }

    public static void main(String[] args) {
        NetcdfDataset ds = NetcdfDataset.openDataset("/Users/greguska/data/mur/20150101090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc")
        println ds.getCoordinateSystems()[0]
        GeoGrid sst = FeatureDatasetFactoryManager.wrap(FeatureDatasetFactoryManager.findFeatureType(ds), ds, null, null).findGridByShortName("analysed_sst")
        GeoGrid subset = sst.subset(null, null, new ucar.ma2.Range(2000, 2010), new ucar.ma2.Range(2500, 2510))

        println subset
    }

}
