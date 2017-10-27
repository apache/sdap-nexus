/*****************************************************************************
* Copyright (c) 2016 Jet Propulsion Laboratory,
* California Institute of Technology.  All rights reserved
*****************************************************************************/
package org.nasa.ingest.tcpshell;

/**
 * Created by greguska on 3/9/16.
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.Lifecycle;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.integration.ip.tcp.connection.TcpConnectionSupport;
import org.springframework.integration.ip.tcp.connection.TcpNetClientConnectionFactory;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Creates a process to run a shell command and communicate with it via a tcp over a port.
 *
 * @author Frank Greguska
 */
public class TcpShellCommand implements Lifecycle, InitializingBean {

    private volatile boolean running = false;

    private final ProcessBuilder processBuilder;

    private volatile Process process;

    private boolean redirectErrorStream;

    private final Map<String, String> environment = new ConcurrentHashMap<>();

    private volatile String workingDirectory;

    private final static Logger log = LoggerFactory.getLogger(TcpShellCommand.class);

    private final TaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();

    private final String command;

    private final Object lifecycleLock = new Object();

    private int timeout = 10000;

    private final int outboundPort;
    private final int inboundPort;
    private final String localhost = "127.0.0.1";

    /**
     * Creates a process to invoke a shell command and then tries to open a connection to the specified port.
     * <p/>
     * Any output from stdout of the process will be logged as INFO.
     * Any output from stderr of the process will be logged as ERROR (unless redirectErrorStream = true).
     *
     * @param command the shell command with command line arguments as separate strings
     * @param outboundPort the port used to send data to the shell process
     * @param inboundPort the port used to receive data from the shell process
     */
    public TcpShellCommand(String command, Integer outboundPort, Integer inboundPort) {
        Assert.hasLength(command, "A shell command is required");
        this.command = command;
        ShellWordsParser shellWordsParser = new ShellWordsParser();
        List<String> commandPlusArgs = shellWordsParser.parse(command);
        Assert.notEmpty(commandPlusArgs, "The shell command is invalid: '" + command + "'");
        processBuilder = new ProcessBuilder(commandPlusArgs);

        Assert.notNull(outboundPort, "outboundPort cannot be null");
        this.outboundPort = outboundPort;

        Assert.notNull(outboundPort, "inboundPort cannot be null");
        this.inboundPort = inboundPort;
    }

    /**
     * Start the process.
     */
    @Override
    public void start() {
        synchronized (lifecycleLock) {
            if (!isRunning()) {

                log.info("starting process. Command = [" + command + "]");

                if (log.isDebugEnabled()) {
                    log.debug("starting process. Command = [" + command + "]");
                }

                try {
                    process = processBuilder.start();
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                    throw new RuntimeException(e.getMessage(), e);
                }

                if (!processBuilder.redirectErrorStream()) {
                    monitorErrorStream();
                }
                monitorProcess();
                monitorStandardOut();


                if(!establishConnection(this.timeout, this.outboundPort)){
                    throw new RuntimeException("Timed out waiting to connect to outbound port " + this.outboundPort);
                }
                if(!establishConnection(this.timeout, this.inboundPort)){
                    throw new RuntimeException("Timed out waiting to connect to inbound port " + this.inboundPort);
                }

                running = true;
                if (log.isDebugEnabled()) {
                    log.debug("process started. Command = [" + command + "]");
                }
            }
        }
    }

    /**
     * Establish a connection on localhost to the specified port within the specified timeout. Will
     * repeatedly try to open a connection until it is successful or the timeout is reached.
     *
     * @param timeout how long to attempt to open a connection
     * @param port port to connect to
     * @return true if connection was successful, false if timed out trying to connect.
     */
    private boolean establishConnection(int timeout, int port){
        TcpNetClientConnectionFactory factory = new TcpNetClientConnectionFactory(this.localhost, port);
        factory.start();
        boolean timedOut = true;
        long start = System.currentTimeMillis();
        try {
            while (System.currentTimeMillis() - start < timeout) {
                TcpConnectionSupport connection = null;
                try {
                    connection = factory.getConnection();
                    timedOut = false;
                    break;
                } catch (Exception e) {
                    //ignore
                } finally {
                    if (connection != null) {
                        connection.close();
                        factory.stop();
                    }
                }

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Interrupted while waiting for socket server to start.", e);
                }
            }
        } finally {
            if (factory.isRunning()) {
                factory.stop();
            }
        }

        return !timedOut;
    }

    /**
     * Wait for the server to start before returning any data.
     */
    public synchronized Message<?> ensureConnected(Message<?> data) {
        if (!isRunning()) {
            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() - start < this.timeout) {
                if (isRunning()) {
                    break;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException("First message interrupted while waiting for socket server to start.", e);
                }
            }
            if (!isRunning()) {
                throw new RuntimeException("Timed out on first message waiting for socket server to start.");
            }
        }

        return data;
    }


    /**
     * Stop the process and close streams.
     */
    @Override
    public void stop() {
        synchronized (lifecycleLock) {
            if (isRunning()) {
                process.destroy();
                running = false;
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /**
     * Set to true to redirect stderr to stdout.
     *
     * @param redirectErrorStream
     */
    public void setRedirectErrorStream(boolean redirectErrorStream) {
        this.redirectErrorStream = redirectErrorStream;
    }

    /**
     * A map containing environment variables to add to the process environment.
     *
     * @param environment
     */
    public void setEnvironment(Map<String, String> environment) {
        this.environment.putAll(environment);
    }

    /**
     * Set the process working directory
     *
     * @param workingDirectory the file path
     */
    public void setWorkingDirectory(String workingDirectory) {
        this.workingDirectory = workingDirectory;
    }

    /**
     * Set the timeout for trying to establish a connection to the configured port
     *
     * @param timeout the amount of time (in milliseconds) to spend trying to connect before throwing an exception
     * @throws Exception
     */
    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        processBuilder.redirectErrorStream(redirectErrorStream);

        if (StringUtils.hasLength(workingDirectory)) {
            processBuilder.directory(new File(workingDirectory));
        }
        if (!CollectionUtils.isEmpty(environment)) {
            processBuilder.environment().putAll(environment);
        }

        //The INBOUND port from the shell's perspective is the OUTBOUND port from the java perspective and vice-versa
        processBuilder.environment().put("INBOUND_PORT", Integer.toString(outboundPort));
        processBuilder.environment().put("OUTBOUND_PORT", Integer.toString(inboundPort));
    }

    /**
     * Runs a thread that waits for the Process result.
     */
    private void monitorProcess() {
        taskExecutor.execute(() -> {
            Process process = TcpShellCommand.this.process;
            if (process == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Process destroyed before starting process monitor");
                }
                return;
            }

            int result;
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Monitoring process '" + command + "'");
                }
                result = process.waitFor();
                if (log.isInfoEnabled()) {
                    log.info("Process '" + command + "' terminated with value " + result);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted - stopping adapter", e);
                stop();
            } finally {
                process.destroy();
            }
        });
    }

    /**
     * Runs a thread that reads stdout.
     */
    private void monitorStandardOut() {
        Process process = this.process;
        if (process == null) {
            if (log.isDebugEnabled()) {
                log.debug("Process destroyed before starting stdout reader");
            }
            return;
        }
        final BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        taskExecutor.execute(() -> {
            String statusMessage;
            if (log.isDebugEnabled()) {
                log.debug("Reading stdout");
            }
            try {
                while ((statusMessage = stdoutReader.readLine()) != null) {
                    log.info(statusMessage);
                }
            } catch (IOException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Exception on process stdout reader", e);
                }
            } finally {
                try {
                    stdoutReader.close();
                } catch (IOException e) {
                    if (log.isDebugEnabled()) {
                        log.debug("Exception while closing stdout", e);
                    }
                }
            }
        });
    }

    /**
     * Runs a thread that reads stderr
     */
    private void monitorErrorStream() {
        Process process = this.process;
        if (process == null) {
            if (log.isDebugEnabled()) {
                log.debug("Process destroyed before starting stderr reader");
            }
            return;
        }
        final BufferedReader errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
        taskExecutor.execute(() -> {
            String statusMessage;
            if (log.isDebugEnabled()) {
                log.debug("Reading stderr");
            }
            try {
                while ((statusMessage = errorReader.readLine()) != null) {
                    log.error(statusMessage);
                }
            } catch (IOException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Exception on process error reader", e);
                }
            } finally {
                try {
                    errorReader.close();
                } catch (IOException e) {
                    if (log.isDebugEnabled()) {
                        log.debug("Exception while closing stderr", e);
                    }
                }
            }
        });
    }

}
