/*****************************************************************************
 * Copyright (c) 2017 Jet Propulsion Laboratory,
 * California Institute of Technology.  All rights reserved
 *****************************************************************************/
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