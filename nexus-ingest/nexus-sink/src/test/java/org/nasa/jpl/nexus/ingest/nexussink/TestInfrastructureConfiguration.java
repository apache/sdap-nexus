/*****************************************************************************
 * Copyright (c) 2016 Jet Propulsion Laboratory,
 * California Institute of Technology.  All rights reserved
 *****************************************************************************/
package org.nasa.jpl.nexus.ingest.nexussink;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.solr.core.SolrOperations;
import org.springframework.data.solr.core.SolrTemplate;

import javax.annotation.Resource;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Created by greguska on 4/4/16.
 */
@Configuration
public class TestInfrastructureConfiguration {

    @Configuration
    @Profile("solr-embedded")
    static class SolrEmbeddedConfiguration {
        @Resource
        private Environment environment;

        @Value("#{environment[T(org.nasa.jpl.nexus.ingest.nexussink.NexusSinkOptionsMetadata).PROPERTY_NAME_SOLR_COLLECTION]}")
        private String solrCollection;

        @Value("#{environment[T(org.nasa.jpl.nexus.ingest.nexussink.NexusSinkOptionsMetadata).PROPERTY_NAME_SOLR_SERVER_URL]}")
        private String solrUrl;

        @Bean
        public SolrClient solrClient() throws IOException {
            Path solrHome = new ClassPathResource("solr").getFile().toPath();
            return new EmbeddedSolrServer(solrHome, solrCollection);
        }

        @Bean
        public SolrOperations solrTemplate(SolrClient solrClient) {
            return new SolrTemplate(solrClient);
        }
    }

}
