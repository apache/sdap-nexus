/*****************************************************************************
* Copyright (c) 2016 Jet Propulsion Laboratory,
* California Institute of Technology.  All rights reserved
*****************************************************************************/
package org.nasa.jpl.nexus.ingest.datatiler

import org.hamcrest.Matcher
import org.junit.Test
import org.springframework.xd.module.ModuleDefinition
import org.springframework.xd.module.ModuleDefinitions
import org.springframework.xd.module.options.DefaultModuleOptionsMetadataResolver
import org.springframework.xd.module.options.ModuleOption
import org.springframework.xd.module.options.ModuleOptionsMetadata
import org.springframework.xd.module.options.ModuleOptionsMetadataResolver

import static org.hamcrest.Matchers.*
import static org.junit.Assert.assertThat
import static org.springframework.xd.module.ModuleType.processor

/**
 * Created by greguska on 3/1/16.
 */
class DatasetTilerPropertiesTest {

    private String moduleName = "datasetTiler";

    @Test
    public void testModuleProperties() {
        ModuleOptionsMetadataResolver moduleOptionsMetadataResolver = new DefaultModuleOptionsMetadataResolver();
        String resource = "classpath:/";
        ModuleDefinition definition = ModuleDefinitions.simple(moduleName, processor, resource);
        ModuleOptionsMetadata metadata = moduleOptionsMetadataResolver.resolve(definition);

        assertThat(
                metadata,
                containsInAnyOrder(moduleOptionNamed("tilesDesired"), moduleOptionNamed("dimensions"), moduleOptionNamed("sliceByDimension"), moduleOptionNamed("splitResult")));
    }

    public static Matcher<ModuleOption> moduleOptionNamed(String name) {
        return hasProperty("name", equalTo(name));
    }

}
