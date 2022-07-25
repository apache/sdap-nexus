analysis
=====

Python module that exposes NEXUS analytical capabilities via a HTTP webservice. Accessible endpoints are described on the [API](https://github.com/dataplumber/nexus/wiki/API) page of the wiki.

# Developer Setup

**NOTE** This project has a dependency on [nexusproto](https://github.com/apache/incubator-sdap-nexusprotohttps://github.com/apache/incubator-sdap-nexusproto). Make sure data-access is installed in the same environment you will be using for this module.

1. Setup a separate conda env or activate an existing one

    ````
    conda create --name nexus-analysis python=3.8
    conda create -n nexus-analysis-38 --no-default-packages python=3.
    conda activate nexus-analysis
    ````

2. Install conda dependencies

    ````
    cd analysis
    conda install -c conda-forge --file conda-requirements.txt
    ````

3. Update the configuration for solr and cassandra

Create .ini files from a copy of their counterpart .ini.default

    analysis/webservice/algorithms/doms/domsconfig.ini
    data-access/nexustiles/config/datastores.ini

This files will override the default values.

They are not saved on git and will not be overridden when you pull the code.

BUT be carefull to remove them when you build the docker image. Otherwise they will be embedded and use in the docker image.

3. install nexusproto

4. install data-access dependency:

    ````
    cd data-access
    pip install cython
    python setup.py install
    ````

5. Launch other requirements, through helm:

   ````
   cd ../helm/
   kubectl create namespace sdap 
   kubectl create configmap collections-config --from-file=../analysis/tests/redirect/ -n sdap
   helm install nexus . -n sdap
   kubectl port-forward service/nexus-cassandra 9042:9042 -n sdap

   ````

7. Set environment variables (examples):

    ```
    PYTHONUNBUFFERED=1
    PROJ_LIB=/opt/anaconda2/envs/nexus-analysis/lib/python2.7/site-packages/pyproj/data
    JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_241.jdk/Contents/Home
     ```

8. Launch unit tests

    pip install pytest
    pip install -e .
    pytest
9. 
    

5. Launch `python webservice/webapp.py` in command line or run it from the IDE.



