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
 * Created by greguska on 3/28/17.
 */
import org.junit.After
import org.junit.Before
import org.junit.Test

import static groovy.test.GroovyAssert.shouldFail
import static org.junit.Assert.assertEquals

class TestAddTimeToSpatialSpec {
    GroovyShell shell
    Binding binding
    PrintStream orig
    ByteArrayOutputStream out

    File script

    @Before
    void setUp() {
        orig = System.out
        out = new ByteArrayOutputStream()
        System.setOut(new PrintStream(out))
        binding = new Binding()
        shell = new GroovyShell(binding)

        script = new File('add-time-to-spatial-spec.groovy')
    }

    @After
    void tearDown() {
        System.setOut(orig)
    }

    @Test
    void testMissingTimeLen() {
        def e = shouldFail RuntimeException, {
            binding.timelen = null
            shell.evaluate(script)
        }
        assert 'This script requires the length of the time array.' == e.message
    }

    @Test
    void testStringPayload() {
        binding.timelen = 4
        binding.payload = "test:0:1,script:3:4"
        binding.headers = ['absolutefilepath': 'afilepath']

        def expected = [
                "time:0:1,test:0:1,script:3:4",
                "time:1:2,test:0:1,script:3:4",
                "time:2:3,test:0:1,script:3:4",
                "time:3:4,test:0:1,script:3:4"
        ].join(';') + ';file://afilepath'

        def result = shell.evaluate(script)
        assertEquals expected, result
    }

    @Test
    void testListPayload() {
        binding.timelen = 2
        binding.payload = ["test:0:1,script:3:4", "test:1:2,script:4:5"]
        binding.headers = ['absolutefilepath': 'afilepath']

        def expected = [
                "time:0:1,test:0:1,script:3:4",
                "time:0:1,test:1:2,script:4:5",
                "time:1:2,test:0:1,script:3:4",
                "time:1:2,test:1:2,script:4:5"
        ].join(';') + ';file://afilepath'

        def result = shell.evaluate(script)
        assertEquals expected, result
    }

}