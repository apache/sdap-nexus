#! /bin/bash

# Start infrastructure
cd /home/ndeploy/nexus/esip-workshop/docker/infrastructure
docker-compose up -d cassandra1

sleep 15

docker-compose up -d

sleep 30

# Try warming up Solr cache
source /home/ndeploy/nexus/esip-workshop/docker/infrastructure/.env
find $HOST_DATA_DIR/solr1/nexustiles_shard2_replica1/data/ -type f -exec cat {} \; > /dev/null
find $HOST_DATA_DIR/solr2/nexustiles_shard1_replica1/data/ -type f -exec cat {} \; > /dev/null
find $HOST_DATA_DIR/solr1/nexustiles_shard3_replica1/data/ -type f -exec cat {} \; > /dev/null

docker exec -it solr1 curl -g 'http://localhost:8983/solr/nexustiles/select?q=dataset_s:AVHRR_OI_L4_GHRSST_NCEI&fq=geo:[40.0,-150.0+TO+55.0,-120.0]&fq={!frange+l%3D0+u%3D0}ms(tile_min_time_dt,tile_max_time_dt)&fq=tile_count_i:[1+TO+*]&fq=tile_min_time_dt:[2013-01-01T00:00:00Z+TO+2016-12-31T00:00:00Z]+&fl=solr_id_s&fl=score&facet=true&facet.field=tile_min_time_dt&facet.limit=-1&f.tile_min_time_dt.facet.mincount=1&f.tile_min_time_dt.facet.limit=-1&start=0&rows=0' > /dev/null
docker exec -it solr1 curl -g 'http://localhost:8983/solr/nexustiles/select?q=dataset_s:TRMM_3B42_daily&fq=geo:[40.0,-150.0+TO+55.0,-120.0]&fq={!frange+l%3D0+u%3D0}ms(tile_min_time_dt,tile_max_time_dt)&fq=tile_count_i:[1+TO+*]&fq=tile_min_time_dt:[2013-01-01T00:00:00Z+TO+2016-12-31T00:00:00Z]+&fl=solr_id_s&fl=score&facet=true&facet.field=tile_min_time_dt&facet.limit=-1&f.tile_min_time_dt.facet.mincount=1&f.tile_min_time_dt.facet.limit=-1&start=0&rows=0' > /dev/null
docker exec -it solr1 curl -g 'http://localhost:8983/solr/nexustiles/select?q=dataset_s:AVHRR_OI_L4_GHRSST_NCEI&fq={!field+f%3Dgeo}Intersects(POLYGON+((-150+40,+-120+40,+-120+55,+-150+55,+-150+40)))&fq=tile_count_i:[1+TO+*]&fq=(tile_min_time_dt:[2013-01-01T00:00:00Z+TO+2016-12-31T00:00:00Z]+OR+tile_max_time_dt:[2013-01-01T00:00:00Z+TO+2016-12-31T00:00:00Z]+OR+(tile_min_time_dt:[*+TO+2013-01-01T00:00:00Z]+AND+tile_max_time_dt:[2016-12-31T00:00:00Z+TO+*]))&fl=*&sort=tile_min_time_dt+asc,tile_max_time_dt+asc' > /dev/null
docker exec -it solr1 curl -g 'http://localhost:8983/solr/nexustiles/select?q=dataset_s:TRMM_3B42_daily&fq={!field+f%3Dgeo}Intersects(POLYGON+((-150+40,+-120+40,+-120+55,+-150+55,+-150+40)))&fq=tile_count_i:[1+TO+*]&fq=(tile_min_time_dt:[2013-01-01T00:00:00Z+TO+2016-12-31T00:00:00Z]+OR+tile_max_time_dt:[2013-01-01T00:00:00Z+TO+2016-12-31T00:00:00Z]+OR+(tile_min_time_dt:[*+TO+2013-01-01T00:00:00Z]+AND+tile_max_time_dt:[2016-12-31T00:00:00Z+TO+*]))&fl=*&sort=tile_min_time_dt+asc,tile_max_time_dt+asc' > /dev/null
docker exec -it solr1 curl -g 'http://localhost:8983/solr/nexustiles/select?q=dataset_s:AVHRR_OI_L4_GHRSST_NCEI&fq={!field+f%3Dgeo}Intersects(POLYGON+((-150+40,+-120+40,+-120+55,+-150+55,+-150+40)))&fq=tile_count_i:[1+TO+*]&fq=(tile_min_time_dt:[2013-01-01T00:00:00Z+TO+2016-12-31T00:00:00Z]+OR+tile_max_time_dt:[2013-01-01T00:00:00Z+TO+2016-12-31T00:00:00Z]+OR+(tile_min_time_dt:[*+TO+2013-01-01T00:00:00Z]+AND+tile_max_time_dt:[2016-12-31T00:00:00Z+TO+*]))&fl=id&sort=tile_min_time_dt+asc,tile_min_lon+asc,tile_min_lat+asc' > /dev/null
docker exec -it solr1 curl -g 'http://localhost:8983/solr/nexustiles/select?q=dataset_s:AVHRR_OI_L4_GHRSST_NCEI_CLIM&fq={!field+f%3Dgeo}Intersects(POLYGON+((-150+40,+-120+40,+-120+55,+-150+55,+-150+40)))&fq=tile_count_i:[1+TO+*]&fl=solr_id_s&fl=score&sort=day_of_year_i+desc' > /dev/null

# Start analysis
cd /home/ndeploy/nexus/esip-workshop/docker/analysis
docker-compose up -d

# Start Jupyter
docker run -d -p 8000:8888 -v /home/ndeploy/nexus/esip-workshop/student-material:/home/jovyan --network infrastructure_nexus --name jupyter nexusjpl/jupyter start-notebook.sh --NotebookApp.password='sha1:1b994085c83b:05d658b3866739ee07f5cb52a67474cdc898840e' --NotebookApp.allow_origin='*'