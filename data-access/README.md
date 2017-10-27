data-access
=====

Python module that provides API access to the NEXUS datastores.

# Developer Setup

## Database Setup

1. Download and unzip [Apache Solr 5.3.1](http://archive.apache.org/dist/lucene/solr/5.3.1/)

    1. Copy the [nexustiles](config/schemas/solr/nexustiles) directory into `$SOLR_INSTALL_DIR/server/solr`
    2. Copy the [dataset](config/schemas/solr/dataset) directory into `$SOLR_INSTALL_DIR/server/solr`
    3. Download [JTS Topology Suite v1.13](https://sourceforge.net/projects/jts-topo-suite/files/jts/1.13/) and extract the zip.
    4. From the exploded JTS zip, copy `$JTS_ZIP/lib/jts-1.13.jar` and `$JTS_ZIP/lib/jtsio-1.13.jar` into `$SOLR_INSTALL_DIR/server/lib/ext`
    5. Start Solr using `$SOLR_INSTALL_DIR/bin/solr start` then open up the admin page (http://localhost:8983) to make sure there are no errors

2. Download and unzip [Apache Cassandra 2.2.x](http://cassandra.apache.org/download/)

    1. Start cassandra `$CASSANDRA_INSTALL_DIR/bin/cassandra`
    2. Open a cqlsh session `$CASSANDRA_INSTALL_DIR/bin/cqlsh`
    3. Execute the DDL located in [nexustiles.cql](config/schemas/cassandra/nexustiles.cql)

## Code Installation

**NOTE** This project has a dependency on [nexus-messages](https://github.jpl.nasa.gov/thuang/nexus/tree/master/nexus-ingest/nexus-messages). Make sure nexus-messages is installed in the same environment you will be using for this module.

1. Setup a separate conda env or activate an existing one

    ````
    conda create --name nexus-data-access python
    source activate nexus-data-access
    ````

2. Install conda dependencies

    ````
    conda install numpy
    ````

3. Install cython `pip install cython`

4. Run `python setup.py install`

5. Run `python test/nexustilestest.py` to validate the installation worked
    1. If you get an error like the following

        ````
        /Users/user/.pyxbld/temp.macosx-10.5-x86_64-2.7/pyrex/cassandra/numpy_parser.c:266:10: fatal error: 'numpyFlags.h' file not found
        ...
        ImportError: Building module nexustiles.dao.CassandraProxy failed: ['ImportError: Building module cassandra.numpy_parser failed: ["CompileError: command \'gcc\' failed with exit status 1\\n"]\n']
        ````

        It can be fixed by copying the `numpyFlags.h` file from the `cassandra` library to the python include library

        ````
        cp /path/to/anaconda/env/lib/python2.7/site-packages/cassandra/numpyFlags.h /path/to/anaconda/env/include/python2.7/numpyFlags.h
        ````