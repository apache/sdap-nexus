FROM jupyter/scipy-notebook

MAINTAINER Apache SDAP "dev@sdap.apache.org"

USER root
RUN apt-get update && \
    apt-get install -y git libgeos-dev
USER jovyan

COPY requirements.txt /tmp
RUN pip install -r /tmp/requirements.txt && \
    conda install -y basemap

ENV CHOWN_HOME_OPTS='-R'
ENV REBUILD_CODE=true
RUN mkdir -p /home/jovyan/Quickstart && \
    mkdir -p /home/jovyan/nexuscli && \
    cd /home/jovyan/nexuscli && \
    git init && \
    git remote add -f origin https://github.com/apache/incubator-sdap-nexus && \
    git config core.sparseCheckout true && \
    echo "client" >> .git/info/sparse-checkout && \
    git pull origin master && \
    cd client && \
    python setup.py install

COPY ["Time Series Example.ipynb", "/home/jovyan/Quickstart/Time Series Example.ipynb"]
