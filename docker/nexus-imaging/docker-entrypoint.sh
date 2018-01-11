#!/bin/bash

#Activate Python environment
source activate nexus

cd /tmp/nexus/analysis/webservice
python WorkflowDriver.py --ds ${DATASET_NAME} --g ${GRANULE_NAME} --p ${PREFIX} --ct ${COLOR_TABLE} --min ${MIN} --max ${MAX} --h ${HEIGHT} --w ${WIDTH} --t ${TIME_INTERVAL} --i ${INTERP}

