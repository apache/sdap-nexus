/*****************************************************************************
* Copyright (c) 2016 Jet Propulsion Laboratory,
* California Institute of Technology.  All rights reserved
*****************************************************************************/
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

    @NotNull
    public String getCassandraContactPoints(){
        return this.cassandraContactPoints;
    }

    @ModuleOption("List of Cassandra hosts")
    public void setCassandraContactPoints(String cassandraContactPoints){
        this.cassandraContactPoints = cassandraContactPoints;
    }

    @NotNull
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


    @AssertTrue(message = "Either "+PROPERTY_NAME_SOLR_SERVER_URL+" or "+PROPERTY_NAME_SOLR_CLOUD_ZK_URL+" is allowed but not both.")
    public boolean isOptionMutuallyExclusive(){
        return Exclusives.atMostOneOf(StringUtils.isNotEmpty(getSolrCloudZkHost()), StringUtils.isNotEmpty(getSolrUrl()));
    }

    @Override
    public String[] profilesToActivate() {
        if(StringUtils.isNotEmpty(getSolrCloudZkHost())){
            return new String[]{"solr-cloud"};
        }else{
            if("http://embedded/".equals(getSolrUrl())){
                return new String[]{"solr-embedded"};
            }else {
                return new String[]{"solr-standalone"};
            }
        }
    }
}
