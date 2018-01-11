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
 * Created by Nga Quach on 8/31/16.
 */

if (timelen == null) {
    throw new RuntimeException("This script requires the length of the time array.")
}
def time = 'time'
if (binding.variables.get("timevar") != null) {
    time = timevar
}

def specsIn = payload
if (payload instanceof String) {
    specsIn = [payload]
}

def length = timelen.toInteger()
def specsOut = []
for (i = 0; i < length; i++) {
    specsOut.addAll(specsIn.collect{ spec ->
        "$time:$i:${i+1},$spec"
    })
}

def sectionSpec = specsOut.join(';')
sectionSpec <<= ';file://'
sectionSpec <<= headers.absolutefilepath

return sectionSpec.toString()
