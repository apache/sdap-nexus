# Purge DOMS/CDMS Execution Data

## Prerequisites

_If you are running the `purge.py` script from within the nexus-webapp-driver Docker image, the following prerequisites are not necessary._

* Run `python setup.py install` in `incubator-sdap-nexus/analysis` 
* Run `pip install -r requirements.txt` in `incubator-sdap-nexus/tools/domspurge`

## Usage
`python purge.py -u USERNAME -p PASSWORD [CASSANDRA ARGS...] (--before DATETIME | --before-months MONTHS | --keep-completed | --all) [--keep-failed] [--dry-run] [-y]`

### Options
- `-u & -p`: Cassandra username and password
- One of:
  - `--before`: Datetime (ie, '2023-04-11T19:50:00') before which all data will be purged. Should be entered in UTC and the timezone must not be specified.
  - `--before-months`: Number of months prior to current date before which all data will be purged. 
  - `--keep-completed`: Keep all completed executions. (only purge uncompleted executions)
  - `--all`: Purge ALL data. (drops and re-creates keyspace)
- Cassandra args (optional)
  - `--cassandra`: Cassandra hostname(s) or IP(s) Can provide a list of args or provide them separated by commas (eg: `host1 host2 host3` or `host1,host2,host3`). (Default: localhost)
  - `-k / --cassandraKeyspace`: Cassandra keyspace for storing DOMS data. (Default: doms)
  - `--cassandraPort`: Port used to connect to Cassandra. (Default: 9042)
  - `--cassandraProtocolVersion`: The version of the Cassandra protocol the driver should use. (Default: 3)
- Additional args
  - `--keep-failed`: Do not purge uncompleted executions (by default all are purged). Incompatible with `--keep-completed`
  - `--dry-run`: Only print the execution ids to be deleted / DB operations to be performed and exit. Do not actually alter the DB
  - `-y / --yes`: Do not prompt user for confirmation.

## Build Docker Image

You can build an image for this script to run it in a Kubernetes CronJob.

```shell
cd /incubator-sdap-nexus
docker build . -f Dockerfile -t sdap-local/DomsPurge:<tag>
```
