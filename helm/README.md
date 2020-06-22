
# NEXUS

NEXUS is an earth science data analytics application, and a component of the [Apache Science Data Analytics Platform (SDAP)](https://sdap.incubator.apache.org/).

## Introduction

The helm chart deploys all the required components of the NEXUS application (Spark webapp, Solr, Cassandra, Zookeeper, and optionally ingress components).

## Table of Contents
- [Prerequisites](#prerequisites)
  - [Spark Operator](#spark-operator)
  - [Persistent Volume Provisioner](#persistent-volume-provisioner)
- [Installing the Chart](#installing-the-chart)
- [Verifying Successful Installation](#verifying-successful-installation)
  - [Local Deployment with Ingress Enabled](#option-1-local-deployment-with-ingress-enabled)
  - [No Ingress Enabled](#option-2-no-ingress-enabled)
- [Uninstalling the Chart](#uninstalling-the-chart)
- [Configuration](#configuration)
- [Restricting Pods to Specific Nodes](#restricting-pods-to-specific-nodes)

## Prerequisites

- Kubernetes 1.9+
- [Spark Operator](https://github.com/helm/charts/tree/master/incubator/sparkoperator)
- Storage class and persistent volume provisioner in the underlying infrastructure

#### Spark Operator
NEXUS needs the [Spark Operator](https://github.com/helm/charts/tree/master/incubator/sparkoperator) in order to run.
Follow their instructions to install the Helm chart, or simply run:

    $ kubectl create namespace spark-operator
    $ helm repo add incubator http://storage.googleapis.com/kubernetes-charts-incubator
    $ helm install incubator/sparkoperator --generate-name --namespace=spark-operator

#### Persistent Volume Provisioner
NEXUS stores data in Cassandra and Solr. In order to have persistent storage, you need to have a Storage Class defined and have Persistent Volumes provisioned either manually or dynamically. See [Persistent Volumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/).
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


## Configuration

There are two ways to override configuration values for the chart. The first is to use the `--set` flag when installing the chart, for example:

    $ helm install nexus incubator-sdap-nexus/helm --namespace=sdap --dependency-update --set cassandra.replicas=3 --set solr.replicas=3

The second way is to create a yaml file with overridden configuration values and pass it in with the `-f` flag during chart installation. 

```yaml
# overridden-values.yml

cassandra:
  replicas: 2
solr:
  replicas: 2
```
```
$ helm install nexus incubator-sdap-nexus/helm --namespace=sdap --dependency-update -f ~/overridden-values.yml
```

The following table lists the configurable parameters of the NEXUS chart and their default values. You can also look at `helm/values.yaml` to see the available options.
> **Note**: The default configuration values are tuned to run NEXUS in a local environment. Setting `ingressEnabled=true` in addition will create a load balancer and expose NEXUS at `localhost`.

|             Parameter                 |            Description             |                    Default                  |
|---------------------------------------|------------------------------------|---------------------------------------------|
| `storageClass`                        | Storage class to use for Cassandra, Solr, and Zookeeper. (Note that `hostpath` should only be used in local deployments.) |`hostpath`|
| `webapp.distributed.image`            | Docker image for the webapp        | `nexusjpl/nexus-webapp:distributed.0.1.3`   |
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
| `cassandra.replicas`                  | Number of Cassandra replicas       | `2`                                         |
| `cassandra.storage`                   | Storage per Cassandra replica      | `13Gi`                                      |
| `cassandra.requests.cpu`              | CPUs to request per Cassandra replica| `1`                                       |
| `cassandra.requests.memory`           | Memory to request per Cassandra replica| `3Gi`                                   |
| `cassandra.limits.cpu`                | CPU limit per Cassandra replica    | `1`                                         |
| `cassandra.limits.memory`             | Memory limit per Cassandra replica | `3Gi`                                       |
| `cassandra.tolerations`               | Tolerations for Cassandra instances| `[]`                                        |
| `cassandra.nodeSelector`              | Node selector for Cassandra instances| `nil`                                     |
| `solr.replicas`                       | Number of Solr replicas (this should not be less than 2, or else solr-cloud will not be happy)| `2`|
| `solr.storage`                        | Storage per Solr replica           | `10Gi`                                      |
| `solr.heap`                           | Heap per Solr replica              | `4g`                                        |
| `solr.requests.memory`                | Memory to request per Solr replica | `5Gi`                                       |
| `solr.requests.cpu`                   | CPUs to request per Solr replica   | `1`                                         |
| `solr.limits.memory`                  | Memory limit per Solr replica      | `5Gi`                                       |
| `solr.limits.cpu`                     | CPU limit per Solr replica         | `1`                                         |
| `solr.tolerations`                    | Tolerations for Solr instances     | `nil`                                       |
| `solr.nodeSelector`                   | Node selector for Solr instances   | `nil`                                       |
| `zookeeper.replicas`                  | Number of zookeeper replicas. This should be an odd number greater than or equal to 3 in order to form a valid quorum.|`3`|
| `zookeeper.memory`                    | Memory per zookeeper replica       | `1Gi`                                       |
| `zookeeper.cpu`                       | CPUs per zookeeper replica         | `0.5`                                       |
| `zookeeper.storage`                   | Storage per zookeeper replica      | `8Gi`                                       |
| `zookeeper.tolerations`               | Tolerations for Zookeeper instances| `nil`                                       |
| `zookeeper.nodeSelector`              | Node selector for Zookeeper instances| `nil`                                     |
| `onEarthProxyIP`                      | IP or hostname to proxy `/onearth` to (leave blank to disable the proxy)| `""`   |
| `ingressEnabled`                      | Enable nginx-ingress               | `false`                                     |
| `nginx-ingress.controller.scope.enabled`|Limit the scope of the ingress controller to this namespace | `true`            |
| `nginx-ingress.controller.kind`       | Install ingress controller as Deployment, DaemonSet or Both	 | `DaemonSet`       |
| `nginx-ingress.controller.service.enabled`| Create a front-facing controller service (this might be used for local or on-prem deployments) | `true` |
| `nginx-ingress.controller.service.type`|Type of controller service to create| `LoadBalancer`                             |
| `nginx-ingress.defaultBackend.enabled`| Use default backend component	     | `false`                                     |


## Restricting Pods to Specific Nodes

Sometimes you may wish to restrict pods to run on specific nodes, for example if you have "UAT" and "SIT" nodes within the same cluster. You can configure 
node selectors and tolerations for all the components, as in the following example:

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
  nodeSelector:
    environment: uat 

solr:
  tolerations:
    - key: environment
      operator: Equal
      value: uat
      effect: NoExecute
  nodeSelector:
    environment: uat 

zookeeper:
  tolerations:
    - key: environment
      operator: Equal
      value: uat
      effect: NoExecute
  nodeSelector:
    environment: uat 
```
>**Note**: The webapp supports `affinity` instead of `nodeSelector` because the Spark Operator has deprecated `nodeSelector` in favor of `affinity`.