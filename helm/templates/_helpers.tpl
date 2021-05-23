{{/* vim: set filetype=mustache: */}}

{{/*
Expand the name of the chart.
*/}}
{{- define "flink-operator.name" -}}
  {{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "flink-operator.fullname" -}}
  {{- if .Values.fullnameOverride -}}
    {{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
  {{- else -}}
    {{- $name := default .Chart.Name .Values.nameOverride -}}
    {{- if contains $name .Release.Name -}}
      {{- .Release.Name | trunc 63 | trimSuffix "-" -}}
    {{- else -}}
      {{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
    {{- end -}}
  {{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "flink-operator.chart" -}}
  {{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create the name of the Flink operator service account to use.
*/}}
{{- define "flink-operator.serviceAccountName" -}}
  {{- if .Values.serviceAccounts.flinkoperator.create -}}
    {{ default (include "flink-operator.fullname" .) .Values.serviceAccounts.flinkoperator.name }}
  {{- else -}}
    {{ default "default" .Values.serviceAccounts.flinkoperator.name }}
  {{- end -}}
{{- end -}}

{{/*
Create the name of the Flink job service account to use.
*/}}
{{- define "flink.serviceAccountName" -}}
  {{- if .Values.serviceAccounts.flink.create -}}
    {{ $flinkServiceaccount := printf "%s-%s" .Release.Name "flink" }}
    {{ default $flinkServiceaccount .Values.serviceAccounts.flink.name }}
  {{- else -}}
    {{ default "default" .Values.serviceAccounts.flink.name }}
  {{- end -}}
{{- end -}}
