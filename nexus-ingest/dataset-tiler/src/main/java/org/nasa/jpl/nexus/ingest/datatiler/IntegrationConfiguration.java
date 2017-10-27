/*****************************************************************************
* Copyright (c) 2016 Jet Propulsion Laboratory,
* California Institute of Technology.  All rights reserved
*****************************************************************************/
package org.nasa.jpl.nexus.ingest.datatiler;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import javax.annotation.Resource;
import java.util.Arrays;

/**
 * Created by greguska on 3/1/16.
 */
@Configuration
@EnableIntegration
public class IntegrationConfiguration {

    @Resource
    private Environment environment;

    @Value("#{environment[T(org.nasa.jpl.nexus.ingest.datatiler.DataTilerOptionsMetadata).PROPERTY_NAME_SPLIT_RESULT]}")
    private Boolean splitResult;

    @Bean
    public MessageChannel input() {
        return new DirectChannel();
    }

    @Bean
    MessageChannel output() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow createTileSpecs(FileSlicer fileSlicer) {
        return IntegrationFlows.from(this.input())
                .transform(inputFile ->
                        fileSlicer.generateSlices(inputFile))
                .<Object, Object>route(payload -> splitResult, mapping -> mapping
                        .channelMapping("false", "output")
                        .subFlowMapping("true", splitResultSubflow -> splitResultSubflow
                                .split()
                        ))
                .channel(this.output())
                .get();
    }


    @Bean
    public TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler tpts = new ThreadPoolTaskScheduler();
        tpts.setPoolSize(1);
        return tpts;
    }

    @Configuration
    @Profile("use-tilesdesired")
    static class SliceByTilesDesiredConfiguration {
        @Resource
        private Environment environment;

        @Value("#{environment[T(org.nasa.jpl.nexus.ingest.datatiler.DataTilerOptionsMetadata).PROPERTY_NAME_TILES_DESIRED]}")
        private Integer tilesDesired;

        @Value("#{environment[T(org.nasa.jpl.nexus.ingest.datatiler.DataTilerOptionsMetadata).PROPERTY_NAME_DIMENSIONS]}")
        private String dimensions;

        @Bean
        public FileSlicer fileSlicer() {
            SliceFileByTilesDesired slicer = new SliceFileByTilesDesired();
            slicer.setTilesDesired(tilesDesired);
            slicer.setDimensions(Arrays.asList(dimensions.split(",")));
            return slicer;
        }
    }

    @Configuration
    @Profile("use-dimension")
    static class SliceByDimensionConfiguration {
        @Resource
        private Environment environment;

        @Value("#{environment[T(org.nasa.jpl.nexus.ingest.datatiler.DataTilerOptionsMetadata).PROPERTY_NAME_SLICE_BY_DIMENSION]}")
        private String sliceByDimension;

        @Value("#{environment[T(org.nasa.jpl.nexus.ingest.datatiler.DataTilerOptionsMetadata).PROPERTY_NAME_DIMENSIONS]}")
        private String dimensions;

        @Bean
        public FileSlicer fileSlicer() {
            SliceFileByDimension slicer = new SliceFileByDimension();
            slicer.setSliceByDimension(sliceByDimension);
            slicer.setDimensions(Arrays.asList(dimensions.split(",")));
            slicer.setDimensionNamePrefix("phony_dim_");
            return slicer;
        }
    }

}
