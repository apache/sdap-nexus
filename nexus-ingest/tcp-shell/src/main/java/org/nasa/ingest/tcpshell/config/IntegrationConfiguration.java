
package org.nasa.ingest.tcpshell.config;

import org.nasa.ingest.tcpshell.TcpShellCommand;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.CustomEditorConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.Environment;
import org.springframework.core.task.TaskExecutor;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.integration.ip.tcp.TcpOutboundGateway;
import org.springframework.integration.ip.tcp.TcpReceivingChannelAdapter;
import org.springframework.integration.ip.tcp.connection.TcpNetClientConnectionFactory;
import org.springframework.integration.ip.tcp.serializer.AbstractByteArraySerializer;
import org.springframework.integration.ip.tcp.serializer.ByteArrayLengthHeaderSerializer;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.xd.extension.process.DelimitedStringToMapPropertyEditor;

import javax.annotation.Resource;
import java.beans.PropertyEditor;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by greguska on 5/31/16.
 */
@Configuration
@EnableIntegration
public class IntegrationConfiguration {

    @Resource
    private Environment environment;

    @Value("#{environment[ T(org.nasa.ingest.tcpshell.config.TcpShellModuleOptionsMetadata).PROPERTY_NAME_SHELL_ENVIRONMENT]?:{:}}")
    private Map<String, String> shellEnvironment;

    @Value("#{environment[ T(org.nasa.ingest.tcpshell.config.TcpShellModuleOptionsMetadata).PROPERTY_NAME_SHELL_COMMAND]}")
    private String shellCommand;

    @Value("#{environment[ T(org.nasa.ingest.tcpshell.config.TcpShellModuleOptionsMetadata).PROPERTY_NAME_WORKING_DIR]}")
    private String workingDirectory;

    @Value("#{environment[ T(org.nasa.ingest.tcpshell.config.TcpShellModuleOptionsMetadata).PROPERTY_NAME_REDIRECT_ERROR_STREAM]}")
    private Boolean redirectErrorStream;

    @Value("#{environment[ T(org.nasa.ingest.tcpshell.config.TcpShellModuleOptionsMetadata).PROPERTY_NAME_SHELL_CONNECT_TIMEOUT]}")
    private Integer shellConnectTimeout;

    @Value("#{environment[ T(org.nasa.ingest.tcpshell.config.TcpShellModuleOptionsMetadata).PROPERTY_NAME_REMOTE_REPLY_TIMEOUT]}")
    private Integer remoteReplyTimeout;

    @Value("${bufferSize}")
    private Integer bufferSize;

    @Value("#{T(org.nasa.ingest.tcpshell.PortResolver).resolvePort(environment[ T(org.nasa.ingest.tcpshell.config.TcpShellModuleOptionsMetadata).PROPERTY_NAME_OUTBOUND_PORT])}")
    private Integer outboundPortNumber;

    @Value("#{T(org.nasa.ingest.tcpshell.PortResolver).resolvePort(environment[ T(org.nasa.ingest.tcpshell.config.TcpShellModuleOptionsMetadata).PROPERTY_NAME_INBOUND_PORT])}")
    private Integer inboundPortNumber;

    @Bean
    @SuppressWarnings("unchecked")
    public TcpShellCommand shellCommand() throws Exception {
        TcpShellCommand command = new TcpShellCommand(shellCommand, outboundPortNumber, inboundPortNumber);
        command.setEnvironment(shellEnvironment);
        command.setWorkingDirectory(workingDirectory);
        command.setRedirectErrorStream(redirectErrorStream);
        command.setTimeout(shellConnectTimeout);

        command.afterPropertiesSet();

        command.start();

        return command;
    }

    @Bean
    @Scope(BeanDefinition.SCOPE_PROTOTYPE)
    public AbstractByteArraySerializer tcpSerializer() {
        ByteArrayLengthHeaderSerializer serializer = new ByteArrayLengthHeaderSerializer();
        serializer.setMaxMessageSize(bufferSize);
        return serializer;
    }

    @Bean
    public TcpNetClientConnectionFactory outboundConnectionFactory() {
        return buildConnectionFactory(outboundPortNumber);
    }

    @Bean
    public TcpNetClientConnectionFactory inboundConnectionFactory() {
        return buildConnectionFactory(inboundPortNumber);
    }

    private TcpNetClientConnectionFactory buildConnectionFactory(Integer portNumber) {
        TcpNetClientConnectionFactory factory = new TcpNetClientConnectionFactory("127.0.0.1", portNumber);
        factory.setSingleUse(false);
        factory.setLookupHost(false);
        factory.setSoKeepAlive(true);
        factory.setSoTcpNoDelay(true);

        TaskExecutor executor = singleThreadExecutor();
        factory.setTaskExecutor(executor);

        AbstractByteArraySerializer serializer = tcpSerializer();
        factory.setDeserializer(serializer);
        factory.setSerializer(serializer);

        factory.afterPropertiesSet();

        return factory;
    }

    @Bean
    public static CustomEditorConfigurer customEditorConfigurer() {
        CustomEditorConfigurer configurer = new CustomEditorConfigurer();
        Map<Class<?>, Class<? extends PropertyEditor>> customEditors = new HashMap<>();
        customEditors.put(Map.class, DelimitedStringToMapPropertyEditor.class);
        configurer.setCustomEditors(customEditors);


        return configurer;
    }

    @Bean
    @Scope(BeanDefinition.SCOPE_PROTOTYPE)
    public TaskExecutor singleThreadExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);
        executor.setQueueCapacity(0);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        executor.afterPropertiesSet();

        return executor;
    }

    @Bean
    public TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(3);
        scheduler.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());

        scheduler.afterPropertiesSet();

        return scheduler;
    }


    @Bean
    public MessageChannel input() {
        return new DirectChannel();
    }

    @Bean
    public TcpOutboundGateway outGateway(TcpNetClientConnectionFactory outboundConnectionFactory) {
        TcpOutboundGateway gateway = new TcpOutboundGateway();
        gateway.setConnectionFactory(outboundConnectionFactory);
        gateway.setRemoteTimeout(remoteReplyTimeout);

        return gateway;
    }

    @Bean
    public IntegrationFlow sendToTcpShell(TcpShellCommand shellCommand, TcpOutboundGateway outGateway) {
        return IntegrationFlows.from(this.input())
                .<Object, Object>route(payload -> shellCommand.isRunning(), mapping -> mapping
                        .channelMapping("true", "outboundTcp")
                        .subFlowMapping("false", serverNotRunningSubflow -> serverNotRunningSubflow
                                .handle(shellCommand::ensureConnected)
                        ))
                .channel(outboundTcp())
                .handle(outGateway)
                .channel("nullChannel")
                .get();
    }

    @Bean
    public TcpReceivingChannelAdapter tcpInboundHandler(TcpNetClientConnectionFactory inboundConnectionFactory, MessageChannel output) {
        TcpReceivingChannelAdapter handler = new TcpReceivingChannelAdapter();
        handler.setClientMode(true);
        handler.setConnectionFactory(inboundConnectionFactory);
        handler.setTaskScheduler(taskScheduler());
        handler.setOutputChannel(output);
        return handler;
    }

    @Bean
    public MessageChannel outboundTcp() {
        return MessageChannels.direct().get();
    }

    @Bean
    public MessageChannel output() {
        return new DirectChannel();
    }
}
