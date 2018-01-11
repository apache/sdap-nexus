
package org.nasa.jpl.nexus.ingest.datatiler

import ucar.nc2.Variable
import ucar.nc2.dataset.NetcdfDataset

/**
 * Created by greguska on 2/1/16.
 */
class SliceFileByDimension implements FileSlicer{

    private String sliceByDimension
    private List<String> dimensions
    private String dimensionNamePrefix

    public void setDimensions(List<String> dims) {
        this.dimensions = dims
    }

    public void setSliceByDimension(String sliceBy){
        this.sliceByDimension = sliceBy
    }

    public void setDimensionNamePrefix(String dimensionNamePrefix){
        this.dimensionNamePrefix = dimensionNamePrefix
    }

    def generateSlices(def inputfile) {

        return sliceByDimension.isInteger()?indexedDimensionSlicing(inputfile):namedDimensionSlicing(inputfile)
    }

    def indexedDimensionSlicing(def inputfile){

        NetcdfDataset ds = null
        def dimensionNameToLength = [:]
        try {
            ds = NetcdfDataset.openDataset(inputfile.getAbsolutePath())

            // Because this is indexed-based dimension slicing, the dimensions are assumed to be unlimited with no names (ie. ds.dimensions == [])
            // Therefore, we need to find a 'representative' variable with dimensions that we can inspect and work with
            // 'lat' and 'lon' are common variable names in the datasets we work with. So try to find one of those first
            // Otherwise, just find the first variable that has the same number of dimensions as was given in this.dimensions
            Variable var = (ds.getVariables().find {"lat".equalsIgnoreCase(it.shortName)})?:(ds.getVariables().find {"lon".equalsIgnoreCase(it.shortName)})?:(ds.getVariables().find { it.getDimensions().size() == this.dimensions.size()})

            assert var != null, "Could not find a variable in ${inputfile.name} with ${dimensions.size()} dimension(s)."

            dimensions.forEach { dim_index ->
                dimensionNameToLength[this.dimensionNamePrefix+dim_index] = var.getDimension(dim_index.toInteger()).length
            }
        } finally {
            ds?.close()
        }

        return generateTileBoundrySlices(this.dimensionNamePrefix+this.sliceByDimension, dimensionNameToLength)

    }

    def namedDimensionSlicing(def inputfile){
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

        return generateTileBoundrySlices(this.sliceByDimension, dimensionNameToLength)
    }

    def generateTileBoundrySlices(sliceByDimension, dimensionNameToLength){
        def combos = dimensionNameToLength.collect { dimensionName, lengthOfDimension ->
            dimensionName.equals(sliceByDimension)?
                    [dimensionName, lengthOfDimension, 1]:
                    [dimensionName, lengthOfDimension, lengthOfDimension]
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

}
