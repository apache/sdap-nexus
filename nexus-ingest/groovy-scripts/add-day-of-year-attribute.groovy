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

/**
 * Created by greguska on 3/29/16.
 */

@Grapes([
        @Grab(group = 'org.nasa.jpl.nexus', module = 'nexus-messages', version = '1.1.0.RELEASE')
])

import org.nasa.jpl.nexus.ingest.wiretypes.NexusContent

if (regex == null ){
    throw new RuntimeException("This script requires a regex to use for matching against the granulename.")
}
if (!(payload instanceof byte[])){
    throw new RuntimeException("Can't handle messages that are not byte[]. Got payload of type ${payload.class}")
}

def pattern = ~"$regex"
def tileBuilder = NexusContent.NexusTile.newBuilder().mergeFrom(payload)

def tileSummary = tileBuilder.summaryBuilder

def granulename = tileSummary.granule

def matches = (granulename =~ pattern)

if (!matches.hasGroup()){
    throw new RuntimeException("regex did not return any groups.")
}
if (1 != matches.size()){
    throw new RuntimeException("regex did not return *one* group.")
}
if (2 != matches[0].size()){
    throw new RuntimeException("group does not contain match.")
}

def dayOfYear = matches[0][1]

tileSummary.addGlobalAttributes(NexusContent.Attribute.newBuilder()
        .setName("day_of_year_i")
        .addValues(dayOfYear.toString())
        .build())

return tileBuilder.build().toByteArray()
