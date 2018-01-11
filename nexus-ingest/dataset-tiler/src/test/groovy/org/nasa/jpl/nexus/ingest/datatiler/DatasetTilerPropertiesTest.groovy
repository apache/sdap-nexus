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
