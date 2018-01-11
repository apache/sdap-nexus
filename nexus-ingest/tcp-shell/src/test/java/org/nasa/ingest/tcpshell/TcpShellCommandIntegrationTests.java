
package org.nasa.ingest.tcpshell;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.xd.dirt.server.singlenode.SingleNodeApplication;
import org.springframework.xd.dirt.test.SingleNodeIntegrationTestSupport;
import org.springframework.xd.dirt.test.SingletonModuleRegistry;
import org.springframework.xd.dirt.test.process.SingleNodeProcessingChain;
import org.springframework.xd.module.ModuleType;
import org.springframework.xd.test.RandomConfigurationSupport;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.springframework.xd.dirt.test.process.SingleNodeProcessingChainSupport.chain;

/**
 * Created by greguska on 3/10/16.
 */
public class TcpShellCommandIntegrationTests {
    private static SingleNodeApplication application;

    private static int RECEIVE_TIMEOUT = 5000;

    private static String moduleName = "tcpshell";

    SingleNodeProcessingChain chain;

    /**
     * Start the single node container, binding random unused ports, etc. to not conflict with any other instances
     * running on this host. Configure the ModuleRegistry to include the project module.
     */
    @BeforeClass
    public static void setUp() {
        RandomConfigurationSupport randomConfigSupport = new RandomConfigurationSupport();
        application = new SingleNodeApplication().run();
        SingleNodeIntegrationTestSupport singleNodeIntegrationTestSupport = new SingleNodeIntegrationTestSupport
                (application);
        singleNodeIntegrationTestSupport.addModuleRegistry(new SingletonModuleRegistry(ModuleType.processor,
                moduleName));

    }

    /**
     * Each test creates a stream with the module under test, or in general a "chain" of processors. The
     * SingleNodeProcessingChain is a test fixture that allows the test to send and receive messages to verify each
     * message is processed as expected.
     */
    @Test
    public void testSpecificPort() throws IOException {

        String streamName = "testDefault";
        File script = new ClassPathResource("echo.py").getFile();
        String command = "python -u " + script.getAbsolutePath();
        String outboundPort = "8820";
        String inboundPort = "8821";

        String payload = "hello";


        String processingChainUnderTest = String.format("%s --command='%s' --outboundPort=%s --inboundPort=%s", moduleName, command, outboundPort, inboundPort);

        chain = chain(application, streamName, processingChainUnderTest);

        chain.sendPayload(payload);
        Object result = chain.receivePayload(RECEIVE_TIMEOUT);

        assertTrue(result instanceof byte[]);
        String responseMessage = new String((byte[]) result, Charset.defaultCharset());
        assertEquals("hello", responseMessage);
    }

    @Test
    public void testAutoPort() throws IOException {

        String streamName = "testDefault";
        File script = new ClassPathResource("echo.py").getFile();
        String command = "python -u " + script.getAbsolutePath();


        String payload = "hello";

        String processingChainUnderTest = String.format("%s --command='%s'", moduleName, command);

        chain = chain(application, streamName, processingChainUnderTest);

        chain.sendPayload(payload);
        Object result = chain.receivePayload(RECEIVE_TIMEOUT);

        assertTrue(result instanceof byte[]);
        String responseMessage = new String((byte[]) result, Charset.defaultCharset());
        assertEquals(payload, responseMessage);
    }

    @Test
    public void testEnvironment() throws IOException {

        String streamName = "testDefault";
        File script = new ClassPathResource("echoenv.py").getFile();
        String command = "python -u " + script.getAbsolutePath();


        String payload = "world";

        String processingChainUnderTest = String.format("%s --command='%s' --environment=%s", moduleName, command, "VAR=hello");

        chain = chain(application, streamName, processingChainUnderTest);

        chain.sendPayload(payload);
        Object result = chain.receivePayload(RECEIVE_TIMEOUT);

        assertTrue(result instanceof byte[]);
        String responseMessage = new String((byte[]) result, Charset.defaultCharset());
        assertEquals("hello world", responseMessage);
    }

    @Test
    public void testManySpringMessages() throws IOException {

        String streamName = "testDefault";
        File script = new ClassPathResource("echo.py").getFile();
        String command = "python -u " + script.getAbsolutePath();


        String payload = "hello";

        String processingChainUnderTest = String.format("%s --command='%s'", moduleName, command);

        chain = chain(application, streamName, processingChainUnderTest);

        for(int x = 0; x < 50; x++) {
            chain.sendPayload(payload);
        }

        for(int x = 0; x < 50; x++) {
            Object result = chain.receivePayload(RECEIVE_TIMEOUT);
            assertNotNull(result);
        }
    }

    @Test
    @Ignore //As written, this test is platform dependent (needs a conda env with correct dependencies installed)
    public void testTileReading() throws IOException {

        String streamName = "testDefault";
        File script = new ClassPathResource("tilereadingprocessor.py").getFile();
        String command = "/Users/greguska/anaconda/envs/nexus-xd-python-modules/bin/python -u " + script.getAbsolutePath();

        File netcdf = new ClassPathResource("netcdf/20150101090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc.nc4").getFile();
        String payload = "time:0:1,lat:0:1,lon:0:1;" + netcdf.getAbsolutePath();


        String processingChainUnderTest = String.format("%s --command='%s' --environment=VARIABLE=analysed_sst,LATITUDE=lat,LONGITUDE=lon,READER=GRIDTILE --bufferSize=10000000", moduleName, command);

        chain = chain(application, streamName, processingChainUnderTest);

        chain.sendPayload(payload);
        Object result = chain.receivePayload(RECEIVE_TIMEOUT);

        assertTrue(result instanceof byte[]);
        String responseMessage = new String((byte[]) result, Charset.defaultCharset());
        System.out.println(responseMessage);
    }

    @Test
    public void testOneSpringMessageMultipleTCPResponses() throws IOException {

        String streamName = "testDefault";
        File script = new ClassPathResource("onetomany.py").getFile();
        String command = "python -u " + script.getAbsolutePath();


        String payload = "hello";

        String processingChainUnderTest = String.format("%s --command='%s'", moduleName, command);

        chain = chain(application, streamName, processingChainUnderTest);

        chain.sendPayload(payload);

        for(int x = 0; x < 20; x++) {
            Object result = chain.receivePayload(RECEIVE_TIMEOUT);
            assertNotNull(result);
            assertTrue(result instanceof byte[]);
            String responseMessage = new String((byte[]) result, Charset.defaultCharset());
            assertTrue(responseMessage.contains(payload));
        }

    }

    /**
     * Destroy the chain to reset message bus bindings and destroy the stream.
     */
    @After
    public void tearDown() {
        if (chain != null) {
            chain.destroy();
        }
    }

}
