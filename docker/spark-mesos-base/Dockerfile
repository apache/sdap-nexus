FROM nexusjpl/nexusbase

MAINTAINER Joseph Jacob "Joseph.Jacob@jpl.nasa.gov"

# Install packages needed for builds

RUN yum install -y gcc python-devel

# Set environment variables.  For Mesos, I used MESOS_VER because MESOS_VERSION
# is expected to be a logical TRUE/FALSE flag that tells Mesos whether or not 
# to simply print the version number and exit.

ENV INSTALL_LOC=/usr/local \
    HADOOP_VERSION=2.7.3 \
    SPARK_VERSION=2.1.0 \
    MESOS_VER=1.2.0 \
    MESOS_MASTER_PORT=5050 \
    MESOS_AGENT_PORT=5051 \
    MESOS_WORKDIR=/var/lib/mesos \
    MESOS_IP=0.0.0.0 \
    MESOS_MASTER_NAME=mesos-master \
    PYTHON_EGG_CACHE=/tmp

ENV CONDA_HOME=${INSTALL_LOC}/anaconda2 \
    MESOS_HOME=${INSTALL_LOC}/mesos-${MESOS_VER} \
    SPARK_DIR=spark-${SPARK_VERSION} \
    SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-hadoop2.7 \
    MESOS_MASTER=mesos://${MESOS_IP}:${MESOS_PORT} \
    MESOS_PACKAGE=mesos-${MESOS_VER}.tar.gz

ENV SPARK_HOME=${INSTALL_LOC}/${SPARK_DIR} \
    PYSPARK_DRIVER_PYTHON=${CONDA_HOME}/bin/python \
    PYSPARK_PYTHON=${CONDA_HOME}/bin/python \
    PYSPARK_SUBMIT_ARGS="--driver-memory=4g pyspark-shell"
    
ENV PYTHONPATH=${PYTHONPATH}:${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.4-src.zip:${SPARK_HOME}/python/lib/pyspark.zip \
    MESOS_NATIVE_JAVA_LIBRARY=${INSTALL_LOC}/lib/libmesos.so \
    
    SPARK_EXECUTOR_URI=${INSTALL_LOC}/${SPARK_PACKAGE}.tgz
    
WORKDIR ${INSTALL_LOC}

# Set up Spark

RUN wget --quiet http://d3kbcqa49mib13.cloudfront.net/${SPARK_PACKAGE}.tgz && \
    tar -xzf ${SPARK_PACKAGE}.tgz && \
    chown -R root.root ${SPARK_PACKAGE} && \
    ln -s ${SPARK_PACKAGE} ${SPARK_DIR}

# Set up Mesos

COPY install_mesos.sh .

RUN source ./install_mesos.sh && \
    mkdir ${MESOS_WORKDIR}

# Set up Anaconda environment
    
ENV PATH=${CONDA_HOME}/bin:${PATH}:${HADOOP_HOME}/bin:${SPARK_HOME}/bin

RUN conda install -c conda-forge -y netCDF4 && \
    conda install -y numpy cython mpld3 scipy basemap gdal matplotlib && \
    pip install shapely cassandra-driver==3.5.0 && \
    conda install -c conda-forge backports.functools_lru_cache=1.3

# Workaround missing libcom_err.so (needed for gdal)

RUN cd /usr/lib64 && ln -s libcom_err.so.2 libcom_err.so.3

# Workaround missing conda libs needed for gdal

RUN cd ${CONDA_HOME}/lib && \
    ln -s libnetcdf.so.11 libnetcdf.so.7 && \
    ln -s libkea.so.1.4.6 libkea.so.1.4.5 && \
    ln -s libhdf5_cpp.so.12 libhdf5_cpp.so.10 && \
    ln -s libjpeg.so.9 libjpeg.so.8

RUN yum install -y mesa-libGL.x86_64

# Retrieve NEXUS code and build it.

WORKDIR /

RUN git clone https://github.com/dataplumber/nexus.git

RUN sed -i 's/,webservice.algorithms.doms//g' /nexus/analysis/webservice/config/web.ini && \
    sed -i 's/127.0.0.1/nexus-webapp/g' /nexus/analysis/webservice/config/web.ini && \
    sed -i 's/127.0.0.1/cassandra1,cassandra2,cassandra3,cassandra4,cassandra5,cassandra6/g' /nexus/data-access/nexustiles/config/datastores.ini && \
    sed -i 's/localhost:8983/solr1:8983/g' /nexus/data-access/nexustiles/config/datastores.ini

WORKDIR /nexus/nexus-ingest/nexus-messages

RUN ./gradlew clean build install

WORKDIR /nexus/nexus-ingest/nexus-messages/build/python/nexusproto

RUN python setup.py install

WORKDIR /nexus/data-access

RUN python setup.py install

WORKDIR /nexus/analysis

RUN python setup.py install

WORKDIR /tmp

CMD ["/bin/bash"]
