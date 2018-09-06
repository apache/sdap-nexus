

This Docker container runs Apache Solr v7.4 as a single node with nexustiles collection.

The easiest way to run it is:

    export SOLR_HOME=/opt/solr/server/solr/
    docker run -it --name solr -e SOLR_HOME=${SOLR_HOME}-v /home/nexus/solr/data:${SOLR_HOME}/nexustiles sdap/solr-singlenode:${VERSION}

/home/nexus/solr/data is directory on host machine where index files will be written to.
