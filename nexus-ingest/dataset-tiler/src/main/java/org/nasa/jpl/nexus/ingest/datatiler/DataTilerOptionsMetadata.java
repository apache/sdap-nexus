/*****************************************************************************
* Copyright (c) 2016 Jet Propulsion Laboratory,
* California Institute of Technology.  All rights reserved
*****************************************************************************/
package org.nasa.jpl.nexus.ingest.datatiler;

import org.springframework.util.StringUtils;
import org.springframework.xd.module.options.spi.ModuleOption;
import org.springframework.xd.module.options.spi.ProfileNamesProvider;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.Pattern;

/**
 * Created by greguska on 3/1/16.
 */
public class DataTilerOptionsMetadata implements ProfileNamesProvider {

    public static final String PROPERTY_NAME_TILES_DESIRED = "tilesDesired";
    public static final String PROPERTY_NAME_DIMENSIONS = "dimensions";

    public static final String PROPERTY_NAME_SLICE_BY_DIMENSION = "sliceByDimension";
    public static final String PROPERTY_NAME_SPLIT_RESULT = "splitResult";

    private Integer tilesDesired;
    private String dimensions;

    private String sliceByDimension;

    private Boolean splitResult = false;

    @ModuleOption("The approximate number of tiles to generate")
    public void setTilesDesired(Integer tilesDesired) {
        this.tilesDesired = tilesDesired;
    }

    @ModuleOption("Comma-delimited list of dimensions to be used for the tiling operation")
    public void setDimensions(String dimensions) {
        this.dimensions = dimensions;
    }

    @ModuleOption("Name of dimension to slice by")
    public void setSliceByDimension(String sliceByDimension) {
        this.sliceByDimension = sliceByDimension;
    }

    @ModuleOption(value = "Should output specifications be split into individual messages", defaultValue = "false")
    public void setSplitResult(Boolean splitResult){ this.splitResult = splitResult; }

    @Min(0)
    @Max(Integer.MAX_VALUE)
    public Integer getTilesDesired() {
        return tilesDesired;
    }

    @Pattern(regexp = "^[-\\w\\s]+(?:,[-\\w\\s]+)*$", flags = {Pattern.Flag.CASE_INSENSITIVE}, message = "Must be a comma-delimited string.")
    public String getDimensions() {
        return dimensions;
    }

    public String getSliceByDimension() {
        return this.sliceByDimension;
    }

    public Boolean getSplitResult() { return this.splitResult; }

    @AssertTrue(message = "'tilesDesired' or 'sliceByDimension' are required and mutually exclusive")
    private boolean isInvalid() {
        return (this.tilesDesired == null && StringUtils.hasText(this.sliceByDimension))
                || (this.tilesDesired != null && !StringUtils.hasText(this.sliceByDimension));
    }

    @Override
    public String[] profilesToActivate() {
        return new String[]{"use-" + (this.getTilesDesired()==null?"dimension":"tilesdesired")};
    }


}
