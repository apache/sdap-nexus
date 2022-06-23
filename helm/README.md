
# NEXUS

NEXUS is an earth science data analytics application, and a component of the [Apache Science Data Analytics Platform (SDAP)](https://sdap.incubator.apache.org/).

## Introduction

The helm chart deploys all the required components of the NEXUS application (Spark webapp, Solr, Cassandra, Zookeeper, and optionally ingress components).

## Table of Contents
- [NEXUS](#nexus)
  - [Introduction](#introduction)
  - [Table of Contents](#table-of-contents)
  - [Prerequisites](#prerequisites)
      - [Spark Operator](#spark-operator)
      - [Persistent Volume Provisioner](#persistent-volume-provisioner)
  - [Installing the Chart](#installing-the-chart)
  - [Verifying Successful Installation](#verifying-successful-installation)
    - [Option 1: Local deployment with ingress enabled](#option-1-local-deployment-with-ingress-enabled)
    - [Option 2: No ingress enabled](#option-2-no-ingress-enabled)
  - [Uninstalling the Chart](#uninstalling-the-chart)
  - [Parameters](#parameters)
    - [SDAP Webapp (Analysis) Parameters](#sdap-webapp-analysis-parameters)
    - [SDAP Ingestion Parameters](#sdap-ingestion-parameters)
    - [Cassandra Parameters](#cassandra-parameters)
    - [Solr/Zookeeper Parameters](#solrzookeeper-parameters)
    - [RabbitMQ Parameters](#rabbitmq-parameters)
    - [Ingress Parameters](#ingress-parameters)
  - [The Collections Config](#the-collections-config)
    - [Option 1: Manually Create a ConfigMap](#option-1-manually-create-a-configmap)
    - [Option 2: Store a File in Git](#option-2-store-a-file-in-git)
  - [Ingestion Sources](#ingestion-sources)
    - [Ingesting from a Local Directory](#ingesting-from-a-local-directory)
    - [Ingesting from S3](#ingesting-from-s3)
    - [Ingesting from an NFS Host](#ingesting-from-an-nfs-host)
  - [Other Configuration Examples](#other-configuration-examples)
    - [Restricting Pods to Specific Nodes](#restricting-pods-to-specific-nodes)
    - [Persistence](#persistence)

## Prerequisites

- Kubernetes 1.9+
- [Spark Operator](https://github.com/helm/charts/tree/master/incubator/sparkoperator)
- Storage class and persistent volume provisioner in the underlying infrastructure

#### Spark Operator
NEXUS needs the [Spark Operator](https://github.com/helm/charts/tree/master/incubator/sparkoperator) in order to run.
Follow their instructions to install the Helm chart, or simply run:

    $ kubectl create namespace spark-operator
    $ helm repo add incubator https://charts.helm.sh/incubator
    $ helm install incubator/sparkoperator --generate-name --namespace=spark-operator

#### Persistent Volume Provisioner
The RabbitMQ, Solr, Zookeeper, Cassandra, and Collection Manager (ingestion) components of SDAP need to be able to store data. In order to have persistent storage, you need to have a Storage Class defined and have Persistent Volumes provisioned either manually or dynamically. See [Persistent Volumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/).
> **Tip**: If you are using an NFS server as storage, you can use [nfs-client-provisioner](https://github.com/helm/charts/tree/master/stable/nfs-client-provisioner) to dynamically provision persistent volumes on your NFS server.


## Installing the Chart

First clone the NEXUS repository.

    $ git clone https://github.com/apache/incubator-sdap-nexus.git

Then install the Helm chart.

    $ kubectl create namespace sdap
    $ helm install nexus incubator-sdap-nexus/helm --namespace=sdap --dependency-update

> **Tip**: It may take a few minutes for the `nexus-webapp-driver` pod to start up because this depends on Solr and Cassandra being accessible.

## Verifying Successful Installation

Check that all the pods are up by running `kubectl get pods -n sdap`, and make sure all pods have status `Running`. If any pods have not started within a few minutes,
you can look at its status with `kubectl describe pod <pod-name> -n sdap.

### Option 1: Local deployment with ingress enabled
If you have installed the Helm chart locally with `ingressEnabled` set to `true` (see `ingressEnabled` under [Configuration](#configuration)), you can verify the installation by requesting the `list` endpoint. If this returns an HTTP 200 response, Nexus is healthy.

    $ curl localhost/nexus/list

### Option 2: No ingress enabled
If you have installed the Helm chart on a cloud provider, and/or have not enabled a loadbalancer with `ingressEnabled=true`, you can temporarily port-forward the nexus-webapp port to see if the webapp responds. 

First, on the Kubernetes cluster or jump host, create a port-forward to the `nexus-webapp` service:

    $ kubectl port-forward service/nexus-webapp -n sdap 8083:8083

Then open another shell on the same host and request the list endpoint through the forwarded port:

    $ curl localhost:8083/list

> **Note**: In this case the list endpoint is `/list` instead of `/nexus/list` because we are connecting to the `nexus-webapp` service directly, instead of through an ingress rule.

If the request returns an HTTP 200 response, NEXUS is healthy. You can now close the first shell to disable the port-forward.

If one of the pods or deployment is not started, you can look at its status with:

    kubectl describe pod <pod-name> -n sdap


## Uninstalling the Chart

To uninstall/delete the `nexus` deployment:

    $ helm delete nexus -n sdap

The command removes all the Kubernetes components associated with the chart and deletes the release.


## Parameters

There are two ways to override configuration parameters for the chart. The first is to use the `--set` flag when installing the chart, for example:

    $ helm install nexus incubator-sdap-nexus/helm --namespace=sdap --dependency-update --set cassandra.replicas=3 --set solr.replicas=3

The second way is to create a yaml file with overridden configuration values and pass it in with the `-f` flag during chart installation. 

```yaml
# overridden-values.yml

cassandra:
  cluster:
    replicaCount: 2
solr:
  replicaCount: 2
```
```
$ helm install nexus incubator-sdap-nexus/helm --namespace=sdap --dependency-update -f ~/overridden-values.yml
```

The following tables list the configurable parameters of the NEXUS chart and their default values. You can also look at `helm/values.yaml` to see the available options.
> **Note**: The default configuration values are tuned to run NEXUS in a local environment. Setting `ingressEnabled=true` in addition will create a load balancer and expose NEXUS at `localhost`.

### SDAP Webapp (Analysis) Parameters
|             Parameter                 |            Description             |                    Default                  |
|---------------------------------------|------------------------------------|---------------------------------------------|
| `onEarthProxyIP`                      | IP or hostname to proxy `/onearth` to (leave blank to disable the proxy)| `""`   |
| `rootWebpage.enabled`                 | Whether to deploy the root webpage (just returns HTTP 200) | `true`              |
| `webapp.enabled`                      | Whether to deploy the webapp       | `true`                                      |
| `webapp.distributed.image`            | Docker image and tag for the webapp| `nexusjpl/nexus-webapp:distributed.0.2.2`   |
| `webapp.distributed.driver.cores`     | Number of cores on Spark driver    | `1`                                         |
| `webapp.distributed.driver.coreLimit` | Maximum cores on Spark driver, in millicpus| `1200m`                             |
| `webapp.distributed.driver.memory`    | Memory on Spark driver             | `512m`                                      |
| `webapp.distributed.driver.tolerations`| Tolerations for Spark driver      | `nil`                                       |
| `webapp.distributed.driver.affinity`  | Affinity (node or pod) for Spark driver| `nil`                                   |
| `webapp.distributed.executor.cores`   | Number of cores on Spark workers   | `1`                                         |
| `webapp.distributed.executor.instances`|Number of Spark workers            | `2`                                         |
| `webapp.distributed.executor.memory`  | Memory on Spark workers            | `512m`                                      |
| `webapp.distributed.executor.tolerations`| Tolerations for Spark workers   | `nil`                                       |
| `webapp.distributed.executor.affinity`| Affinity (node or pod) for Spark workers| `nil`                                  |


### SDAP Ingestion Parameters
|             Parameter                 |            Description             |                    Default                  |
|---------------------------------------|------------------------------------|---------------------------------------------|
| `ingestion.enabled`                   | Enable ingestion by deploying the Config Operator, Collection Manager, Granule Ingestion| `true` |
| `ingestion.granuleIngester.replicas`  | Number of Granule Ingester replicas | `2`                                        |
| `ingestion.granuleIngester.image`     | Docker image and tag for Granule Ingester| `nexusjpl/granule-ingester:0.1.2`     |
| `ingestion.granuleIngester.cpu`       | CPUs (request and limit) for each Granule Ingester replica| `1`                  |
| `ingestion.granuleIngester.memory`    | Memory (request and limit) for each Granule Ingester replica| `1Gi`              |
| `ingestion.collectionManager.image`   | Docker image and tag for Collection Manager| `nexusjpl/collection-manager:0.1.2` |
| `ingestion.collectionManager.cpu`     | CPUs (request and limit) for the Collection Manager | `0.5`                      |
| `ingestion.collectionManager.memory`  | Memory (request and limit) for the Collection Manager | `1Gi`                    |
| `ingestion.configOperator.image`      | Docker image and tag for Config Operator | `nexusjpl/config-operator:0.0.1`      |
| `ingestion.granules.nfsServer`        | An optional URL to an NFS server containing a directory where granule files are stored. If set, this NFS server will be mounted in the Collection Manager and Granule Ingester pods.| `nil` |
| `ingestion.granules.mountPath`        | The path in the Collection Manager and Granule Ingester pods where granule files will be mounted. *Important:* the `path` property on all collections in the Collections Config file should match this value.| `/data` |
| `ingestion.granules.path`             | Directory on either the local filesystem or an NFS mount where granule files are located. This directory will be mounted onto the Collection Manager and Granule Ingester at `ingestion.granules.mountPath`. | `/var/lib/sdap/granules` |
| `ingestion.granules.s3.bucket`        | An optional S3 bucket from which to download granules for ingestion. If this is set, `ingestion.granules.nfsServer` and `ingestion.granules.path` will be ignored.|`nil`|
| `ingestion.granules.awsCredsEnvs`     | Environment variables containing AWS credentials. This should be populated if `ingestion.granules.s3.bucket` is set. See https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html for possible options.|`nil`|
| `ingestion.collections.createCrd`     | Whether to automatically create the `GitBasedConfig` CRD (custom resource definition). This CRD is only needed if loading the Collections Config from a Git repository is enabled (i.e., only if `ingestion.collections.git.url` is set).
| `ingestion.collections.configMap`     | The name of the ConfigMap to use as the [Collections Config](https://github.com/apache/incubator-sdap-ingester/tree/dev/collection_manager#the-collections-configuration-file). If this is set, then the Config Operator and its associated RBAC roles and CRD will **not** be deployed. Correspondingly, if you do not want to use a Git repository to store the Collections Config, this value should be set. | `nil`|
| `ingestion.collections.git.url`       | URL to a Git repository containing a [Collections Config](https://github.com/apache/incubator-sdap-ingester/tree/dev/collection_manager#the-collections-configuration-file) file. The file should be at the root of the repository. The repository URL should be of the form `https://github.com/username/repo.git`.| `nil`|
| `ingestion.collections.git.branch`    | Branch to use when loading a Collections Config file from a Git repository.| `master`|
| `ingestion.history.solrEnabled`       | Whether to store ingestion history in Solr, instead of in a filesystem directory. If this is set to `true`, `ingestion.history.storageClass` will be ignored. | `true`|
| `ingestion.history.storageClass`       | The storage class to use for storing ingestion history files. This will only be used if `ingestion.history.solrEnabled` is set to `false`. | `hostpath`|


### Cassandra Parameters

See the [Cassandra Helm chart docs](https://github.com/bitnami/charts/tree/master/bitnami/cassandra) for full list of options. 
|             Parameter                 |            Description             |                    Default                  |
|---------------------------------------|------------------------------------|---------------------------------------------|
| `cassandra.enabled`                   | Whether to deploy Cassandra        | `true`                                      |
| `cassandra.initDBConfigMap`           | Configmap for initialization CQL commands (done in the first node) | `init-cassandra`|
| `cassandra.dbUser.user`               | Cassandra admin user               | `cassandra`                                 |
| `cassandra.dbUser.password`           | Password for `dbUser.user`. Randomly generated if empty| `cassandra`             |
| `cassandra.cluster.replicaCount`      | Number of Cassandra replicas       | `1`                                         |
| `cassandra.persistence.storageClass`  | PVC Storage Class for Cassandra data volume| `hostpath`                          |
| `cassandra.persistence.size`          | PVC Storage Request for Cassandra data volume| `8Gi`                             |
| `cassandra.resources.requests.cpu`    | CPUs to request per Cassandra replica| `1`                                       |
| `cassandra.resources.requests.memory` | Memory to request per Cassandra replica| `8Gi`                                   |
| `cassandra.resources.limits.cpu`      | CPU limit per Cassandra replica    | `1`                                         |
| `cassandra.resources.limits.memory`   | Memory limit per Cassandra replica | `8Gi`                                       |
| `external.cassandraHost`              | External Cassandra host for if `cassandra.enabled` is set to `false`. This should be set if connecting SDAP to a Cassandra database that is not deployed by the SDAP Helm chart. | `nil`|
| `external.cassandraUsername`          | Optional Cassandra username, only applies if `external.cassandraHost` is set.| `nil`|
| `external.cassandraPassword`          | Optional Cassandra password, only applies if `external.cassandraHost` is set.| `nil`|


### Solr/Zookeeper Parameters

See the [Solr Helm chart docs](https://github.com/helm/charts/tree/master/incubator/solr) and [Zookeeper Helm chart docs](https://github.com/helm/charts/tree/master/incubator/zookeeper) for full set of options. 
|             Parameter                 |            Description             |                    Default                  |
|---------------------------------------|------------------------------------|---------------------------------------------|
| `solr.enabled`                        | Whether to deploy Solr and Zookeeper| `true`                                     |
| `solr.initPodEnabled`                 | Whether to deploy a pod which initializes the Solr database for SDAP (does nothing if the database is alreday initialized)| `true`|
| `solr.image.repository`               | The repository to pull the Solr docker image from| `nexusjpl/solr`               |
| `solr.image.tag`                      | The tag on the Solr repository to pull| `8.4.0`                                  |
| `solr.replicaCount`                   | The number of replicas in the Solr statefulset| `3`                              |
| `solr.volumeClaimTemplates.storageClassName`| The name of the storage class for the Solr PVC| `hostpath`                 |
| `solr.volumeClaimTemplates.storageSize`| The size of the PVC               | `10Gi`                                      |
| `solr.resources.requests.memory`      | Memory to request per Solr replica | `2Gi`                                       |
| `solr.resources.requests.cpu`         | CPUs to request per Solr replica   | `1`                                         |
| `solr.resources.limits.memory`        | Memory limit per Solr replica      | `2Gi`                                       |
| `solr.resources.limits.cpu`           | CPU limit per Solr replica         | `1`                                         |
| `solr.zookeeper.replicaCount`         | The number of replicas in the Zookeeper statefulset (this should be an odd number)| `3`|
| `solr.zookeeper.persistence.storageClass`| The name of the storage class for the Zookeeper PVC| `hostpath`               |
| `solr.zookeeper.resources.requests.memory`| Memory to request per Zookeeper replica| `1Gi`                               |
| `solr.zookeeper.resources.requests.cpu`| CPUs to request per Zookeeper replica| `0.5`                                    |
| `solr.zookeeper.resources.limits.memory`| Memory limit per Zookeeper replica| `1Gi`                                      |
| `solr.zookeeper.resources.limits.cpu` | CPU limit per Zookeeper replica    | `0.5`                                       |
| `external.solrHostAndPort`            | External Solr host for if `solr.enabled` is set to `false`. This should be set if connecting SDAP to a Solr database that is not deployed by the SDAP Helm chart. | `nil`|
| `external.zookeeperHostAndPort`       | External Zookeeper host for if `solr.enabled` is set to `false`. This should be set if connecting SDAP to a Solr database and Zookeeper that is not deployed by the SDAP Helm chart. | `nil`|


### RabbitMQ Parameters

See the [RabbitMQ Helm chart docs](https://github.com/bitnami/charts/tree/master/bitnami/rabbitmq) for full set of options. 
|             Parameter                 |            Description             |                    Default                  |
|---------------------------------------|------------------------------------|---------------------------------------------|
| `rabbitmq.enabled`                    | Whether to deploy RabbitMQ         | `true`                                      |
| `rabbitmq.persistence.storageClass`   | Storage class to use for RabbitMQ  | `hostpath`                                  |
| `rabbitmq.replicaCount`               | Number of RabbitMQ replicas        | `1`                                         |
| `rabbitmq.auth.username`              | RabbitMQ username                  | `guest`                                     |
| `rabbitmq.auth.password`              | RabbitMQ password                  | `guest`                                     |
| `rabbitmq.ingress.enabled`            | Enable ingress resource for RabbitMQ Management console | `true`                 |


### Ingress Parameters

See the [nginx-ingress Helm chart docs](https://github.com/helm/charts/tree/master/stable/nginx-ingress) for full set of options. 
|             Parameter                 |            Description             |                    Default                  |
|---------------------------------------|------------------------------------|---------------------------------------------|
| `nginx-ingress.enabled`               | Whether to deploy nginx ingress controllers| `false`                             |
| `nginx-ingress.controller.scope.enabled`|Limit the scope of the ingress controller to this namespace | `true`            |
| `nginx-ingress.controller.kind`       | Install ingress controller as Deployment, DaemonSet or Both  | `DaemonSet`       |
| `nginx-ingress.controller.service.enabled`| Create a front-facing controller service (this might be used for local or on-prem deployments) | `true` |
| `nginx-ingress.controller.service.type`|Type of controller service to create| `LoadBalancer`                             |
| `nginx-ingress.defaultBackend.enabled`| Use default backend component	     | `false`                                     |

## The Collections Config

In order to ingest data into SDAP, you must write a Collections Config. This is a YAML-formatted configuration which defines 
what granules to ingest into which collections (or "datasets"), and how. See the [Collections Manager docs](https://github.com/apache/incubator-sdap-ingester/tree/dev/collection_manager#the-collections-configuration-file) for information on the proper content and format of the Collections Config.

There are two ways to manage the Collections Config:

### Option 1: Manually Create a ConfigMap
Create a [ConfigMap](https://kubernetes.io/docs/concepts/configuration/configmap/) by hand, containing the collections config YAML under a key called `collections.yml`. Then set the Chart configuration option `ingestion.collections.configMap` to the name of the ConfigMap.

### Option 2: Store a File in Git
Write a Collections Config YAML file, save it as `collections.yml`, check it into a Git repository under the root directory, and let the [Config Operator](https://github.com/apache/incubator-sdap-ingester/tree/dev/config_operator) create the ConfigMap for you. 
The Config Operator will periodically read the YAML file from Git, and create or update a ConfigMap with the contents of the file. 

To enable this, set `ingestion.collections.git.url` to the Git URL of the repository containing the Collections Config file.

## Ingestion Sources

SDAP supports ingesting granules from either a local directory, an AWS S3 bucket, or an NFS server. (It is not yet possible to configure SDAP to ingest from multiple of these sources simultanously.)

### Ingesting from a Local Directory

To ingest granules that are stored on the local filesystem, you must provide the path to the directory where the granules are stored. This directory will be mounted as a volume in the ingestion pods.
> **Note**: if you are ingesting granules that live on the local filesystem, the granule files must be accessible at the same location on every Kubernetes node
> that the collections-manager and granule-ingester pods are running on. Because of this, it usually only makes sense to use local directory ingestion if a) your Kubernetes cluster consists of a single node (as in the case of running Kubernetes on a local computer), or b) you have configured nodeAffinity to force 
> the collections-manager and granule-ingester pods to run on only one node (see [Restricting Pods to Specific Nodes](#restricting-pods-to-specific-nodes)).

The following is an example configuration for ingesting granules from a local directory: 

```yaml
ingestion:
  granules:
    path: /share/granules
    mountPath: /data
```

The `ingestion.granules.mountPath` property sets the mount path in the ingestion pods where the granule directory will be mounted. **The root directory of the `path` property of all collection entries in the collections config must match this value.** This is because the `path` property of collections in the collections config describes to the 
ingestion pods where to find the mounted granules.

The following is an example of a collections config to be used with the NFS ingestion configuration above: 

```yaml
# collections.yml

collections:
  - id: "CSR-RL06-Mascons_LAND"
    path: "/data/CSR-RL06-Mascons-land/CSR_GRACE_RL06_Mascons_v01-land.nc" 
    priority: 1
    projection: Grid
    dimensionNames:
      latitude: lat
      longitude: lon
      time: time
      variable: lwe_thickness
    slices:
      time: 1
      lat: 60
      lon: 60
  - id: "TELLUS_GRAC-GRFO_MASCON_CRI_GRID_RL06_V2_LAND"
    path: "/data/grace-fo-land/"
    priority: 1
    projection: Grid
    dimensionNames:
      latitude: lat
      longitude: lon
      time: time
      variable: lwe_thickness
    slices:
      time: 1
      lat: 60
      lon: 60
```

In addition, renote collections, handled by an atlernate SDAP deployment, can be configured, as follow:

```yaml

  - id: gmu-pm25
    path: https://aq-sdap.stcenter.net/nexus/
    remote-id: PM25

```

This collections will be proposed in the /list end point, as follow:

```json
  ...
, {
    "shortName": "gmu-no2", 
    "remoteUrl": "https://aq-sdap.stcenter.net/nexus/", 
    "remoteShortName": "NO2"
  }
  ...
```

The collection can be requested on the main SDAP, with any client library supporting HTTP redirection. The most commonly library used as SDAP clients: python's `requests` or javascript's `XMLHTTPRequest` support the redirection by default.

For example the remote collection can be requested with usual requests on the main SDAP server, for example: 

https://digitaltwin.jpl.nasa.gov/nexus/timeSeriesSpark?ds=gmu-no2&minLon=-118.31&minLat=33.61&maxLon=-118.09&maxLat=33.87&startTime=2021-10-01T00:00:00Z&endTime=2021-12-31T00:00:00Z

Seamlessly, transparently from the user point of view, the processing of the timeSeriesSpark or any other algorithm will be invoked on the alternate SDAP server (deployed on https://aq-sdap.stcenter.net/nexus/). The invocation of the alternate SDAP is done automatically by an HTTP redirection directive (status 302) sent to the client.  


### Ingesting from S3

To ingest granules that are stored in an S3 bucket, you must provide the name of the S3 bucket to read from, as well as the S3 credentials as environment variables.
(See the [AWS docs](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html) for the list of possible AWS credentials environment variables.)

The following is an example configuration that enables ingestion from S3: 

```yaml
ingestion:
  granules:
    s3:
      bucket: my-nexus-bucket
      awsCredsEnvs:
        AWS_ACCESS_KEY_ID: my-secret
        AWS_SECRET_ACCESS_KEY: my-secret
        AWS_DEFAULT_REGION: us-west-2
```

When S3 ingestion is enabled, the `path` property of all collection entries in the collections config must be an S3 path or prefix. (Due to S3 limitations, wildcards are not supported.) The following
is an example of a collections config to be used with the S3 ingestion configuration above: 

```yaml
# collections.yml

collections:
  - id: "CSR-RL06-Mascons_LAND"
    path: "s3://my-nexus-bucket/CSR-RL06-Mascons-land/CSR_GRACE_RL06_Mascons_v01-land.nc" # full S3 path
    priority: 1
    projection: Grid
    dimensionNames:
      latitude: lat
      longitude: lon
      time: time
      variable: lwe_thickness
    slices:
      time: 1
      lat: 60
      lon: 60
  - id: "TELLUS_GRAC-GRFO_MASCON_CRI_GRID_RL06_V2_LAND"
    path: "s3://my-nexus-bucket/grace-fo-land/" # S3 prefix
    priority: 1
    projection: Grid
    dimensionNames:
      latitude: lat
      longitude: lon
      time: time
      variable: lwe_thickness
    slices:
      time: 1
      lat: 60
      lon: 60
```

### Ingesting from an NFS Host

To ingest granules that are stored on an NFS host, you must provide the NFS host url, and the path to the directory on the NFS server the granules are located.

The following is an example configuration that enables ingestion from an NFS host: 

```yaml
ingestion:
  granules:
    nfsServer: nfsserver.example.com
    path: /share/granules
    mountPath: /data
```

The `ingestion.granules.mountPath` property sets the mount path in the ingestion pods where the granule directory will be mounted. **The root directory of the `path` property of all collection entries in the collections config must match this value.** This is because the `path` property of collections in the collections config describes to the 
ingestion pods where to find the mounted granules.

The following is an example of a collections config to be used with the NFS ingestion configuration above: 

```yaml
# collections.yml

collections:
  - id: "CSR-RL06-Mascons_LAND"
    path: "/data/CSR-RL06-Mascons-land/CSR_GRACE_RL06_Mascons_v01-land.nc" 
    priority: 1
    projection: Grid
    dimensionNames:
      latitude: lat
      longitude: lon
      time: time
      variable: lwe_thickness
    slices:
      time: 1
      lat: 60
      lon: 60
  - id: "TELLUS_GRAC-GRFO_MASCON_CRI_GRID_RL06_V2_LAND"
    path: "/data/grace-fo-land/"
    priority: 1
    projection: Grid
    dimensionNames:
      latitude: lat
      longitude: lon
      time: time
      variable: lwe_thickness
    slices:
      time: 1
      lat: 60
      lon: 60
```

## Other Configuration Examples

### Restricting Pods to Specific Nodes

Sometimes you may wish to restrict pods to run on specific nodes, for example if you have "UAT" and "SIT" nodes within the same cluster. You can configure 
affinity and tolerations for all the components, as in the following example:

```yaml
webapp:
  distributed:
    driver:
      tolerations:
        - key: environment
          operator: Equal
          value: uat
          effect: NoExecute
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
            - matchExpressions:
              - key: environment
                operator: In
                values:
                - uat
    executor:
      tolerations:
        - key: environment
          operator: Equal
          value: uat
          effect: NoExecute
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
            - matchExpressions:
              - key: environment
                operator: In
                values:
                - uat

cassandra:
  tolerations:
    - key: environment
      operator: Equal
      value: uat
      effect: NoExecute
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: environment
            operator: In
            values:
            - uat

solr:
  tolerations:
    - key: environment
      operator: Equal
      value: uat
      effect: NoExecute
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: environment
            operator: In
            values:
            - uat
  zookeeper:
    tolerations:
      - key: environment
        operator: Equal
        value: uat
        effect: NoExecute
    affinity:
      nodeAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
          - matchExpressions:
            - key: environment
              operator: In
              values:
              - uat
```

### Persistence

The SDAP Helm chart uses [persistent volumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) for RabbitMQ, Solr, Zookeeper, Cassandra, and optionally the Collection Manager ingestion component (if Solr ingestion history is disabled).
In most use cases you will want to use the same storage class for all of these components.

For example, if you are deploying SDAP on AWS and you want to use [EBS gp2](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-volume-types.html#EBSVolumeTypes_gp2) volumes for persistence storage, you would need to set the following 
configuration values for the SDAP Helm chart:

```yaml
rabbitmq:
  persistence:
    storageClass: gp2

cassandra:
  persistence:
    storageClass: gp2 

solr:
  volumeClaimTemplates:
    storageClassName: gp2 
  zookeeper:
    persistence:
      storageClass: gp2

ingestion:
  history:
    storageClass: gp2 # This is only needed if Solr ingestion history is disabled, as follows:
    solrEnabled: false 
```
