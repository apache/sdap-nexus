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

if (!(payload instanceof byte[])){
    throw new RuntimeException("Can't handle messages that are not byte[]. Got payload of type ${payload.class}")
}
thesalt = binding.variables.get("salt")


def tileBuilder = NexusContent.NexusTile.newBuilder().mergeFrom(payload)
def summaryBuilder = tileBuilder.getSummaryBuilder()

def granule = summaryBuilder.hasGranule()?summaryBuilder.granule:"${headers['absolutefilepath']}".split(File.separator)[-1]
def originalSpec = summaryBuilder.hasSectionSpec()?summaryBuilder.sectionSpec:"${headers['spec']}"


def tileId = UUID.nameUUIDFromBytes("${granule[0..-4]}$originalSpec${thesalt==null?'':thesalt}".toString().bytes).toString()

summaryBuilder.setGranule(granule)
summaryBuilder.setTileId(tileId)
summaryBuilder.setSectionSpec(originalSpec)

tileBuilder.getTileBuilder().setTileId(tileId)

return tileBuilder.build().toByteArray()

