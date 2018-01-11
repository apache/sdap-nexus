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

package org.nasa.jpl.nexus.ingest.nexussink;


import com.google.protobuf.InvalidProtocolBufferException;
import org.nasa.jpl.nexus.ingest.wiretypes.NexusContent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.env.Environment;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.config.IntegrationConverter;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.Collection;

/**
 * Created by greguska on 3/1/16.
 */
@Configuration
@EnableIntegration
@Import(InfrastructureConfiguration.class)
public class IntegrationConfiguration {

    @Resource
    private Environment environment;

    @Value("#{environment[T(org.nasa.jpl.nexus.ingest.nexussink.NexusSinkOptionsMetadata).PROPERTY_NAME_INSERT_BUFFER]}")
    private Integer insertBuffer;

    private static final Integer GROUP_TIMEOUT_MS = 2000;

    @Autowired
    private MetadataStore metadataStore;

    @Autowired
    private DataStore dataStore;

    @Bean
    public MessageChannel input() {

        return MessageChannels.direct().get();

    }

    @Bean
    public IntegrationFlow routeInput( MessageChannel input, NexusService nexus, Converter byteArrayToNexusTileConverter ) {

        return IntegrationFlows.from(input)
                .<NexusContent.NexusTile, Boolean>route(NexusContent.NexusTile.class, payload -> insertBuffer > 1, mapping -> mapping
                    .subFlowMapping("true", aggregateflow -> aggregateflow
                        .transform(NexusContent.NexusTile.class, m -> m)
                        .<NexusContent.NexusTile, Collection<NexusContent.NexusTile>>aggregate(a -> a
                                .correlationStrategy(message -> 0) //all messages are part of the same group for now. TODO Could implement table-specific groups here
                                .releaseStrategy(group -> group.size() >= insertBuffer)
                                .sendPartialResultOnExpiry(true)
                                .expireGroupsUponCompletion(true)
                                .expireGroupsUponTimeout(true)
                                .groupTimeout(GROUP_TIMEOUT_MS)))
                    .subFlowMapping("false", single -> single
                        .<NexusContent.NexusTile, Collection<NexusContent.NexusTile>>transform(NexusContent.NexusTile.class, Arrays::asList)
                    ))
                .handle(Collection.class, (nexusTiles, headers) ->
                        nexus.saveToNexus(nexusTiles))
                .get();
    }

    @Bean
    public NexusService nexus() {
        return new NexusService(metadataStore, dataStore);
    }

    @Bean
    @IntegrationConverter
    public Converter byteArrayToNexusTileConverter() {
        return new Converter<byte[], NexusContent.NexusTile>() {
            @Override
            public NexusContent.NexusTile convert(byte[] source) {

                try {
                    return NexusContent.NexusTile.newBuilder().mergeFrom(source).build();
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException("Could not convert message.", e);
                }
            }
        };
    }

    @Bean
    public TaskScheduler taskScheduler(){
        ThreadPoolTaskScheduler tpts = new ThreadPoolTaskScheduler();
        tpts.setPoolSize(5);
        return tpts;
    }


}
