/*****************************************************************************
* Copyright (c) 2016 Jet Propulsion Laboratory,
* California Institute of Technology.  All rights reserved
*****************************************************************************/
package org.nasa.jpl.nexus.ingest.nexussink;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.data.cassandra.config.CassandraClusterFactoryBean;
import org.springframework.data.cassandra.config.CassandraSessionFactoryBean;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.solr.core.SolrOperations;
import org.springframework.data.solr.core.SolrTemplate;

import javax.annotation.Resource;

import static org.nasa.jpl.nexus.ingest.nexussink.NexusSinkOptionsMetadata.*;

/**
 * Created by greguska on 4/4/16.
 */
@Configuration
public class InfrastructureConfiguration {

    @Configuration
    @Profile("cassandra")
    static class CassandraConfiguration {
        @Resource
        private Environment environment;

        @Bean
        public CassandraClusterFactoryBean cluster() {

            CassandraClusterFactoryBean cluster = new CassandraClusterFactoryBean();
            cluster.setContactPoints(environment.getRequiredProperty(PROPERTY_NAME_CASSANDRA_CONTACT_POINTS));
            cluster.setPort(Integer.parseInt(environment.getProperty(PROPERTY_NAME_CASSANDRA_PORT)));

            return cluster;
        }

        @Bean
        public CassandraMappingContext mappingContext() {
            return new BasicCassandraMappingContext();
        }

        @Bean
        public CassandraConverter converter() {
            return new MappingCassandraConverter(mappingContext());
        }

        @Bean
        public CassandraSessionFactoryBean session() throws Exception {

            CassandraSessionFactoryBean session = new CassandraSessionFactoryBean();
            session.setCluster(cluster().getObject());
            session.setKeyspaceName(environment.getRequiredProperty(PROPERTY_NAME_CASSANDRA_KEYSPACE));
            session.setConverter(converter());
            session.setSchemaAction(SchemaAction.NONE);

            return session;
        }

        @Bean
        public CassandraOperations cassandraTemplate() throws Exception {
            return new CassandraTemplate(session().getObject());
        }

        @Bean
        public DataStore dataStore(CassandraOperations cassandraTemplate) {
            DataStore dataStore = new CassandraStore(cassandraTemplate);
            return dataStore;
        }
    }

    @Configuration
    @Profile("solr-standalone")
    static class SolrStandaloneConfiguration{
        @Resource
        private Environment environment;

        @Value("#{environment[T(org.nasa.jpl.nexus.ingest.nexussink.NexusSinkOptionsMetadata).PROPERTY_NAME_SOLR_COLLECTION]}")
        private String solrCollection;

        @Value("#{environment[T(org.nasa.jpl.nexus.ingest.nexussink.NexusSinkOptionsMetadata).PROPERTY_NAME_SOLR_SERVER_URL]}")
        private String solrUrl;

        @Bean
        public SolrClient solrClient(){ return new HttpSolrClient(solrUrl+solrCollection);}

        @Bean
        public SolrOperations solrTemplate(SolrClient solrClient) {
            return new SolrTemplate(solrClient);
        }

        @Bean
        public MetadataStore metadataStore(SolrOperations solrTemplate) {
            MetadataStore metadataStore = new SolrStore(solrTemplate);
            return metadataStore;
        }
    }


    @Configuration
    @Profile("solr-cloud")
    static class SolrCloudConfiguration {
        @Resource
        private Environment environment;

        @Value("#{environment[T(org.nasa.jpl.nexus.ingest.nexussink.NexusSinkOptionsMetadata).PROPERTY_NAME_SOLR_COLLECTION]}")
        private String solrCollection;

        @Value("#{environment[T(org.nasa.jpl.nexus.ingest.nexussink.NexusSinkOptionsMetadata).PROPERTY_NAME_SOLR_CLOUD_ZK_URL]}")
        private String solrCloudZkHost;

        @Bean
        public SolrClient solrClient(){
            CloudSolrClient client = new CloudSolrClient(solrCloudZkHost);
            //client.setIdField("dataset_s");
            client.setDefaultCollection(solrCollection);

            return client;
        }

        @Bean
        public SolrOperations solrTemplate(SolrClient solrClient) {
            return new SolrTemplate(solrClient);
        }

        @Bean
        public MetadataStore metadataStore(SolrOperations solrTemplate) {
            MetadataStore metadataStore = new SolrStore(solrTemplate);
            return metadataStore;
        }
    }

    @Configuration
    @Profile("s3")
    static class S3Configuration {
        @Value("#{environment[T(org.nasa.jpl.nexus.ingest.nexussink.NexusSinkOptionsMetadata).PROPERTY_NAME_S3_BUCKET]}")
        private String s3BucketName;

        @Value("#{environment[T(org.nasa.jpl.nexus.ingest.nexussink.NexusSinkOptionsMetadata).PROPERTY_NAME_AWS_REGION]}")
        private String s3Region;

        @Bean
        public AmazonS3Client s3client() {
            AmazonS3Client s3Client = new AmazonS3Client();
            s3Client.setRegion(Region.getRegion(Regions.fromName(s3Region)));
            return s3Client;
        }

        @Bean
        public DataStore dataStore(AmazonS3Client s3Client) {
            S3Store s3Store = new S3Store(s3Client, s3BucketName);
            return s3Store;
        }
    }

    @Configuration
    @Profile("dynamo")
    static class DynamoConfiguration {
        @Value("#{environment[T(org.nasa.jpl.nexus.ingest.nexussink.NexusSinkOptionsMetadata).PROPERTY_NAME_DYNAMO_TABLE_NAME]}")
        private String dynamoTableName;

        @Value("#{environment[T(org.nasa.jpl.nexus.ingest.nexussink.NexusSinkOptionsMetadata).PROPERTY_NAME_AWS_REGION]}")
        private String dynamoRegion;

        @Bean
        public AmazonDynamoDB dynamoClient() {
            AmazonDynamoDB dynamoClient = new AmazonDynamoDBClient();
            dynamoClient.setRegion(Region.getRegion(Regions.fromName(dynamoRegion)));
            return dynamoClient;
        }

        @Bean
        public DataStore dataStore(AmazonDynamoDB dynamoClient) {
            DynamoStore dynamoStore = new DynamoStore(dynamoClient, dynamoTableName);
            return dynamoStore;
        }
    }
}
