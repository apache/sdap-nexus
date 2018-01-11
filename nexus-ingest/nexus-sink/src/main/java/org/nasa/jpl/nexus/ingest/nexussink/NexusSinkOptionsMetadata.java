
package org.nasa.jpl.nexus.ingest.nexussink;

import org.apache.commons.lang.StringUtils;
import org.springframework.xd.module.options.spi.ModuleOption;
import org.springframework.xd.module.options.spi.ProfileNamesProvider;
import org.springframework.xd.module.options.validation.Exclusives;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * Created by greguska on 3/1/16.
 */
public class NexusSinkOptionsMetadata implements ProfileNamesProvider{

    public static final String PROPERTY_NAME_SOLR_SERVER_URL = "solrUrl";
    public static final String PROPERTY_NAME_SOLR_CLOUD_ZK_URL = "solrCloudZkHost";
    public static final String PROPERTY_NAME_SOLR_COLLECTION = "solrCollection";

    public static final String PROPERTY_NAME_CASSANDRA_CONTACT_POINTS = "cassandraContactPoints";
    public static final String PROPERTY_NAME_CASSANDRA_KEYSPACE = "cassandraKeyspace";
    public static final String PROPERTY_NAME_CASSANDRA_PORT = "cassandraPort";

    public static final String PROPERTY_NAME_INSERT_BUFFER = "insertBuffer";

    public static final String PROPERTY_NAME_S3_BUCKET = "s3BucketName";
    public static final String PROPERTY_NAME_AWS_REGION = "awsRegion";
    public static final String PROPERTY_NAME_DYNAMO_TABLE_NAME = "dynamoTableName";

    private String s3BucketName = "";
    private String awsRegion = "";
    private String dynamoTableName = "";
    private String dataStore = "";

    private String cassandraContactPoints = null;
    private String cassandraKeyspace = null;
    private Integer cassandraPort = 9042;

    private String solrUrl = "";
    private String solrCloudZkHost = "";
    private String solrCollection = null;

    private Integer insertBuffer = 0;

    /*
     * Cassandra settings
     */

    public String getCassandraContactPoints(){
        return this.cassandraContactPoints;
    }

    @ModuleOption("List of Cassandra hosts")
    public void setCassandraContactPoints(String cassandraContactPoints){
        this.cassandraContactPoints = cassandraContactPoints;
    }

    public String getCassandraKeyspace(){
        return this.cassandraKeyspace;
    }

    @ModuleOption("Cassandra keyspace")
    public void setCassandraKeyspace(String cassandraKeyspace){
        this.cassandraKeyspace = cassandraKeyspace;
    }

    @NotNull
    public Integer getCassandraPort(){
        return this.cassandraPort;
    }

    @ModuleOption(value = "Port used to connect to Cassandra", defaultValue = "9042")
    public void setCassandraPort(Integer cassandraPort){
        this.cassandraPort = cassandraPort;
    }

    /*
     * SOLR settings
     */

    public String getSolrUrl(){
        return this.solrUrl;
    }

    @ModuleOption(value = "the URL for connecting to Solr (ex. http://solrhost1:8983/solr/). This or "+PROPERTY_NAME_SOLR_CLOUD_ZK_URL+" should be set but not both", defaultValue = "")
    public void setSolrUrl(String solrUrl){
        if(StringUtils.isNotEmpty(solrUrl) && !solrUrl.endsWith("/")){
            this.solrUrl = solrUrl+"/";
        }else {
            this.solrUrl = solrUrl;
        }
    }

    public String getSolrCloudZkHost(){ return this.solrCloudZkHost; }

    @ModuleOption(value = "list of zk hosts that are hosting solr cloud (ex. zkhost1:2181,zkhost2:2181/solr). This or "+PROPERTY_NAME_SOLR_SERVER_URL+" should be set but not both", defaultValue = "")
    public void setSolrCloudZkHost(String solrCloudZkHost){
        this.solrCloudZkHost = solrCloudZkHost;
    }

    @NotNull
    public String getSolrCollection(){ return this.solrCollection; }

    @ModuleOption(value = "the name of the solr collection or core to use for storage")
    public void setSolrCollection(String solrCollection){
        this.solrCollection = solrCollection;
    }

    /*
     * Other settings
     */

    @Min(0)
    public Integer getInsertBuffer(){ return this.insertBuffer; }

    @ModuleOption(value = "number of messages to buffer before inserting into Nexus", defaultValue = "0")
    public void setInsertBuffer(Integer insertBuffer){ this.insertBuffer = insertBuffer; }

    @ModuleOption(value = "The name of the S3 bucket", defaultValue = "nexus-jpl")
    public void setS3BucketName(String s3BucketName) {
        this.s3BucketName = s3BucketName;
    }

    public String getS3BucketName() {
        return this.s3BucketName;
    }

    @ModuleOption(value = "The AWS region", defaultValue = "us-west-2")
    public void setAwsRegion(String awsRegion) {
        this.awsRegion = awsRegion;
    }

    public String getAwsRegion() {
        return this.awsRegion;
    }

    @ModuleOption(value = "The name of the dynamoDB table", defaultValue = "nexus-jpl-table")
    public void setDynamoTableName(String dynamoTableName) {
        this.dynamoTableName = dynamoTableName;
    }

    public String getDynamoTableName() {
        return this.dynamoTableName;
    }

    @AssertTrue(message = "Either "+PROPERTY_NAME_CASSANDRA_KEYSPACE+", "+PROPERTY_NAME_S3_BUCKET+", or "
            +PROPERTY_NAME_DYNAMO_TABLE_NAME+" is allowed but not more than 1.")
    public boolean isOptionMutuallyExclusiveDataStore() {
        return Exclusives.atMostOneOf(StringUtils.isNotEmpty(getCassandraKeyspace()), StringUtils.isNotEmpty(getS3BucketName()),
                StringUtils.isNotEmpty(getDynamoTableName()));
    }

    @AssertTrue(message = "Both "+PROPERTY_NAME_CASSANDRA_KEYSPACE+" and "+PROPERTY_NAME_CASSANDRA_CONTACT_POINTS+
            " are required if using Cassandra.")
    public boolean isCassandraConfigured() {
        if (StringUtils.isEmpty(getCassandraKeyspace())) {
            return true; //If Cassandra isn't used, return true to avoid test failures
        }

        return StringUtils.isNotEmpty(getCassandraContactPoints());
    }

    @AssertTrue(message = "Both "+PROPERTY_NAME_S3_BUCKET+" and "+PROPERTY_NAME_AWS_REGION+" are required if using S3.")
    public boolean isS3Configured() {
        if (StringUtils.isEmpty(getS3BucketName())) {
            return true; //If S3 isn't used, return true to avoid test failures
        }

        return StringUtils.isNotEmpty(getAwsRegion());
    }

    @AssertTrue(message = "Both "+PROPERTY_NAME_DYNAMO_TABLE_NAME+" and "+PROPERTY_NAME_AWS_REGION+" are required if using DynamoDB.")
    public boolean isDynamoConfigured() {
        if (StringUtils.isEmpty(getDynamoTableName())) {
            return true; //If DynamoDB isn't used, return true to avoid test failures
        }

        return StringUtils.isNotEmpty(getAwsRegion());
    }

    @AssertTrue(message = "Either "+PROPERTY_NAME_SOLR_SERVER_URL+" or "+PROPERTY_NAME_SOLR_CLOUD_ZK_URL+" is allowed but not both.")
    public boolean isOptionMutuallyExclusiveMetadataStore() {
        return Exclusives.atMostOneOf(StringUtils.isNotEmpty(getSolrCloudZkHost()), StringUtils.isNotEmpty(getSolrUrl()));
    }

    @Override
    public String[] profilesToActivate() {
        String[] profiles = new String[2];

        if (StringUtils.isNotEmpty(getSolrCloudZkHost())) {
            profiles[0] = "solr-cloud";
        }
        else {
            if ("http://embedded/".equals(getSolrUrl())) {
                profiles[0] = "solr-embedded";
            }
            else {
                profiles[0] = "solr-standalone";
            }
        }
        if (StringUtils.isNotEmpty(getCassandraKeyspace())) {
            profiles[1] = "cassandra";
        }
        else {
            if (StringUtils.isNotEmpty(getS3BucketName())) {
                if (StringUtils.isNotEmpty(getAwsRegion())) {
                    profiles[1] = "s3";
                }
                else {
                    profiles[1] = "s3local";
                }
            }
            else {
                profiles[1] = "dynamo";
            }
        }

        return profiles;
    }
}
