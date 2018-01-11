
package org.nasa.ingest.tcpshell;

/**
 * Created by greguska on 3/10/16.
 */

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Frank Greguska
 */
public class TcpShellCommandTests {

    private TcpShellCommand scp = null;

    @Rule
    public PythonAvailableRule pythonAvailableRule = new PythonAvailableRule();

    @BeforeClass
    public static void init() {
        File file = new File("src/test/resources/echo.py");
        assertTrue(file.exists());
    }

    @Test
    public void startTest() throws Exception {
        scp = new TcpShellCommand("python src/test/resources/echo.py", 8009, 8010);
        scp.afterPropertiesSet();
        scp.start();
        assertTrue(scp.isRunning());
        scp.stop();
    }

}
