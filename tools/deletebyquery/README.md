# Deleting Datasets from SDAP

## Prerequisites

_If you are running the `deletebyquery.py` script from within the nexus-webapp-driver Docker image, the following prerequisites are not necessary._

* Run `python setup.py install` in `incubator-sdap-nexus/data-access` 
* Run `python setup.py install` in `incubator-sdap-nexus/analysis` 
* Run `pip install -r requirements.txt` in `incubator-sdap-nexus/tools/deletebyquery`

## Running the Script
_Note: It is recommended to run this from within the nexus-webapp-driver Docker image._

```
python deletebyquery.py --solr <solr host>:8983 --cassandra <cassandra host> --cassandraUsername <cassandra username> --cassandraPassword <cassandra password> --query '<solr query>'
```

Run `python deletebyquery.py` without any arguments to see the full list of options.

## Usage Example
To delete a dataset called `my_dataset`, with SDAP deployed using the Helm chart, run the following from within the nexus-webapp-driver Docker image:
```
cd /incubator-sdap-nexus/tools/deletebyquery
python deletebyquery.py --solr sdap-solr-svc:8983 --cassandra sdap-cassandra --cassandraUsername cassandra --cassandraPassword cassandra --query 'dataset_s:"my_dataset"'
```

You can provide a flag `-f` or `--force` which will cause the script to skip all prompts before deleting. 