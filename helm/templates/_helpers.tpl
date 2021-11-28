{{/* vim: set filetype=mustache: */}}

{{/*
Name of the generated configmap containing the contents of the collections config file.
*/}}
{{- define "nexus.collectionsConfig.configmapName" -}}
{{ .Values.ingestion.collections.configMap | default "collections-config" }}
{{- end -}}

{{/*
Path at which to mount the collections config file in the Collection Manager.
*/}}
{{- define "nexus.collectionsConfig.mountPath" -}}
/var/lib/sdap/collections
{{- end -}}

{{/*
Path at which to mount the history directory in the Collection Manager, if not using Solr for history.
*/}}
{{- define "nexus.history.mountPath" -}}
/var/lib/sdap/history
{{- end -}}


{{/*
The data volume which is used in both the Collection Manager and the Granule Ingester.
*/}}
{{- define "nexus.ingestion.dataVolume" -}}
- name: data-volume
  {{- if .Values.ingestion.granules.nfsServer }}
  nfs:
    server: {{ .Values.ingestion.granules.nfsServer }}
    path: {{ .Values.ingestion.granules.path }}
  {{- else }}
  {{- if .Values.ingestion.granules.path }}
  hostPath:
    path: {{ .Values.ingestion.granules.path }}
  {{- end -}}
  {{- end -}}
{{- end -}}

{{/*
The data volume mount which is used in both the Collection Manager and the Granule Ingester.
*/}}
{{- define "nexus.ingestion.dataVolumeMount" -}}
- name: data-volume
  mountPath: {{ .Values.ingestion.granules.mountPath }}
{{- end -}}

{{- define "nexus.urls.solr" -}}
{{ .Values.external.solrHostAndPort | default (print "http://" .Release.Name "-solr:8983") }}
{{- end -}}

{{- define "nexus.urls.zookeeper" -}}
{{ .Values.external.zookeeperHostAndPort | default (print .Release.Name "-zookeeper:2181") }}
{{- end -}}

{{- define "nexus.urls.cassandra" -}}
{{ .Values.external.cassandraHost | default (print .Release.Name "-cassandra") }}
{{- end -}}

{{- define "nexus.credentials.cassandra.username" -}}
{{ .Values.external.cassandraUsername | default "cassandra" }}
{{- end -}}

{{- define "nexus.credentials.cassandra.password" -}}
{{ .Values.external.cassandraPassword | default "cassandra" }}
{{- end -}}
