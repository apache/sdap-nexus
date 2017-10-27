FROM jupyter/scipy-notebook

USER root
RUN apt-get update && \
    apt-get install -y git libgeos-dev
USER jovyan

COPY requirements.txt /tmp
RUN pip install -r /tmp/requirements.txt && \
    conda install -y basemap

ENV REBUILD_CODE=truee
RUN mkdir -p /home/jovyan/nexuscli && \
    cd /home/jovyan/nexuscli && \
    git init && \
    git remote add -f origin https://github.com/dataplumber/nexus && \
    git config core.sparseCheckout true && \
    echo "client" >> .git/info/sparse-checkout && \
    git pull origin master && \
    cd client && \
    python setup.py install

