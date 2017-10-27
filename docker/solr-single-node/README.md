

This Docker container runs Apache Solr v6.4.1 as a single node with nexustiles collection.

The easiest way to run it is:

    docker run --net=host --name nexus-solr -v /home/nexus/solr/data:/opt/solr/server/solr/nexustiles/data -d nexusjpl/nexus-solr-single-node

/home/nexus/solr/data is directory on host machine where index files will be written to.
