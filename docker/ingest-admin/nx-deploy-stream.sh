#!/bin/bash

. /usr/local/nx-env.sh

if [ $# -gt 0 ]; then
  while true; do  
    case "$1" in
        --datasetName)
            DATASET_NAME="$2" 
            shift 2
        ;;
        --dataDirectory)
            DATA_DIR="$2"
            shift 2
        ;;
        --variableName)
            VARIABLE="$2"
            shift 2
        ;;
        --tilesDesired)
            TILES_DESIRED="$2"
            shift 2
        ;;
        *)
            break # out-of-args, stop looping

        ;;
    esac
  done
fi

echo "stream create --name ${DATASET_NAME} --definition \"scan-for-avhrr-granules: file --dir=${DATA_DIR} --mode=ref --pattern=*.nc --maxMessages=1 --fixedDelay=1 | header-absolutefilepath: header-enricher --headers={\\\"absolutefilepath\\\":\\\"payload\\\"} | dataset-tiler --dimensions=lat,lon --tilesDesired=${TILES_DESIRED} | join-with-static-time: transform --expression=\\\"'time:0:1,'+payload.stream().collect(T(java.util.stream.Collectors).joining(';time:0:1,'))+';file://'+headers['absolutefilepath']\\\" | python-chain: tcpshell --command='python -u -m nexusxd.processorchain' --environment=CHAIN=nexusxd.tilereadingprocessor.read_grid_data:nexusxd.emptytilefilter.filter_empty_tiles:nexusxd.kelvintocelsius.transform:nexusxd.tilesumarizingprocessor.summarize_nexustile,VARIABLE=${VARIABLE},LATITUDE=lat,LONGITUDE=lon,TIME=time,READER=GRIDTILE,TEMP_DIR=/tmp,STORED_VAR_NAME=${VARIABLE} --bufferSize=1000000 --remoteReplyTimeout=360000 | add-id: script --script=file:///usr/local/spring-xd/current/xd-nexus-shared/generate-tile-id.groovy | set-dataset-name: script --script=file:///usr/local/spring-xd/current/xd-nexus-shared/set-dataset-name.groovy --variables='datasetname=${DATASET_NAME}' | nexus --cassandraContactPoints=${CASS_HOST} --cassandraKeyspace=nexustiles --solrCloudZkHost=${SOLR_CLOUD_ZK_HOST} --solrCollection=nexustiles --cassandraPort=${CASS_PORT}\"" > /tmp/stream-create

xd-shell --cmdfile /tmp/stream-create

echo "stream deploy --name ${DATASET_NAME} --properties module.python-chain.count=3,module.nexus.count=3" > /tmp/stream-deploy

xd-shell --cmdfile /tmp/stream-deploy
