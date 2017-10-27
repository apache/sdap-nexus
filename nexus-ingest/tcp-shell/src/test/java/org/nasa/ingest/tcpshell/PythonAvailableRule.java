/*****************************************************************************
* Copyright (c) 2016 Jet Propulsion Laboratory,
* California Institute of Technology.  All rights reserved
*****************************************************************************/
package org.nasa.ingest.tcpshell;

/**
 * Created by greguska on 3/10/16.
 */
import org.springframework.xd.test.AbstractExternalResourceTestSupport;

/**
 * @author David Turanski
 */
public class PythonAvailableRule extends AbstractExternalResourceTestSupport<Object> {
    private final ProcessBuilder processBuilder;

    protected PythonAvailableRule() {
        super("python command");
        processBuilder = new ProcessBuilder("python");
    }

    @Override
    protected void cleanupResource() throws Exception {

    }

    @Override
    protected void obtainResource() throws Exception {
        Process process = null;
        try {
            process = processBuilder.start();
        }
        finally {
            if (process != null) {
                process.destroy();
            }
        }
    }
}
