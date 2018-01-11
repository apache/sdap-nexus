
package org.nasa.ingest.tcpshell.config;

/**
 * Created by greguska on 3/9/16.
 */

import org.hibernate.validator.constraints.NotEmpty;
import org.springframework.xd.module.options.mixins.FromStringCharsetMixin;
import org.springframework.xd.module.options.spi.Mixin;
import org.springframework.xd.module.options.spi.ModuleOption;
import org.springframework.xd.module.options.spi.ProfileNamesProvider;
import org.springframework.xd.tcp.encdec.EncoderDecoderMixins.BufferSizeMixin;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

/**
 * Options Metadata for tcp shell processor
 *
 * @author Frank Greguska
 */
@Mixin({BufferSizeMixin.class})
public class TcpShellModuleOptionsMetadata {

    public static final String PROPERTY_NAME_SHELL_COMMAND = "command";
    public static final String PROPERTY_NAME_SHELL_ENVIRONMENT = "environment";

    public static final String PROPERTY_NAME_WORKING_DIR = "workingDir";
    public static final String PROPERTY_NAME_REDIRECT_ERROR_STREAM = "redirectErrorStream";

    public static final String PROPERTY_NAME_OUTBOUND_PORT = "outboundPort";
    public static final String PROPERTY_NAME_INBOUND_PORT = "inboundPort";

    public static final String PROPERTY_NAME_SHELL_CONNECT_TIMEOUT = "shellConnectTimeout";
    public static final String PROPERTY_NAME_REMOTE_REPLY_TIMEOUT = "remoteReplyTimeout";

    private String command;

    private String environment;

    private String workingDir;

    private boolean redirectErrorStream;

    private String outboundPort = null;

    private String inboundPort = null;

    private Integer shellConnectTimeout = 15000;

    private Integer remoteReplyTimeout = 180000;

    @ModuleOption("additional process environment variables as comma delimited name-value pairs")
    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    public String getEnvironment() {
        return environment;
    }

    @NotEmpty
    @NotNull
    public String getCommand() {
        return command;
    }

    @ModuleOption("the shell command")
    public void setCommand(String command) {
        this.command = command;
    }

    public String getWorkingDir() {
        return workingDir;
    }

    @ModuleOption("the process working directory")
    public void setWorkingDir(String workingDir) {
        this.workingDir = workingDir;
    }

    public boolean isRedirectErrorStream() {
        return redirectErrorStream;
    }

    @ModuleOption("redirects stderr to stdout")
    public void setRedirectErrorStream(boolean redirectErrorStream) {
        this.redirectErrorStream = redirectErrorStream;
    }

    @ModuleOption(value = "port used for sending data to the shell process", defaultValue = "automatic")
    public void setOutboundPort(String outboundPort) {
        this.outboundPort = outboundPort;
    }

    @Min(value = 1024)
    @Max(value = 65535)
    public String getOutboundPort() {
        return this.outboundPort;
    }

    @ModuleOption(value = "port used for receiving data from the shell process", defaultValue = "automatic")
    public void setInboundPort(String inboundPort) {
        this.inboundPort = inboundPort;
    }

    @Min(value = 1024)
    @Max(value = 65535)
    public String getInboundPort() {
        return this.inboundPort;
    }

    @ModuleOption(value = "max time (in ms) to wait to connect to shell process", defaultValue = "15000")
    public void setShellConnectTimeout(Integer shellConnectTimeout){
        this.shellConnectTimeout = shellConnectTimeout;
    }

    public Integer getShellConnectTimeout(){
        return this.shellConnectTimeout;
    }

    @ModuleOption(value = "max time (in ms) to wait for a reply from the shell process after sending a message", defaultValue = "180000")
    public void setRemoteReplyTimeout(Integer remoteReplyTimeout){ this.remoteReplyTimeout = remoteReplyTimeout; }

    public Integer getRemoteReplyTimeout(){ return this.remoteReplyTimeout; }
}
