{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}


{{/*
Create a resource name of format:
  <global.namespace>-<global.releaseTag>-<global.name>

The default name can be overidded by setting nameOverride in values

*/}}
{{- define "resource.releaseTag" -}}
{{- default "staging" .Values.global.releaseTag | trunc 63 | trimSuffix "-" -}}
{{- end -}}


{{- define "resource.namespace" -}}
{{- $namespacePrefix := include "resource.releaseTag" . -}}
{{- $namespaceSuffix := default "test-namespace" .Values.global.namespacePrefix | trunc 63 | trimSuffix "-" -}}
{{- printf "%s-%s" $namespacePrefix $namespaceSuffix | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}


{{- define "chart.name" -}}
{{- default .Chart.Name .Values.chartName | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "strategy.name" -}}
{{- $strategy := default "test-strat" .Values.global.strategyName | trunc 63 | trimSuffix "-" -}}
{{- printf "%s" $strategy  | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "resource.name" -}}
{{- $strategyName := include "strategy.name" . -}}
{{- printf "%s" $strategyName | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}


{{- define "resource.appName" -}}
{{- $chartName := include "chart.name" . -}}
{{- default $chartName .Values.global.appName | trunc 63 | trimSuffix "-" -}}
{{- end -}}


{{- define "image.hummingbot.repository" -}}
{{- $repository :=  .Values.global.image.hummingbot.repository }}
{{- $tag :=  include "resource.releaseTag" . }}
{{- printf "%s:%s" $repository $tag | trunc 63 | trimSuffix "-" -}}
{{- end -}}



{{- define "node.default" -}}
{{- $releaseTag :=  include "resource.releaseTag" . }}
{{- $node := "" -}}
{{- if eq $releaseTag "release" -}}
{{- $node := .Values.global.node.release.default.name -}}
{{-  printf "%s" $node | trunc 63 | trimSuffix "-" -}}
{{- else if eq $releaseTag  "staging" -}}
{{- $node := .Values.global.node.staging.default.name -}}
{{-  printf "%s" $node | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $node := .Values.global.node.develop.default.name -}}
{{-  printf "%s" $node | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}


{{- define "node.storage" -}}
{{- $releaseTag :=  include "resource.releaseTag" . }}
{{- $node := "" -}}
{{- if eq $releaseTag "release" -}}
{{- $node := .Values.global.node.release.storage.name -}}
{{- printf "%s" $node | trunc 63 | trimSuffix "-" -}}
{{- else if eq $releaseTag  "staging" -}}
{{- $node := .Values.global.node.staging.storage.name -}}
{{- printf "%s" $node | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $node := .Values.global.node.develop.storage.name -}}
{{- printf "%s" $node | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}


{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "chart.fullname" -}}
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
{{- define "chart.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Get the concatenated key data for a given strategy. 

Each account's data property is a comma seperated set of file names to contents. 
ie. "key file 1 name":"key file 1 contents","key file 2 name":"key file 2 contents",...

This function returns a string combination of the data entries for each account name that is passed in 
from the list argument "accountList". These data entries are combined, have the last comma removed, 
and are then wrapped in angle brackets "{ ... }", to make a valid json dict of those keyfiles.

Expects a dict as an argument with the entry "accountList".
*/}}
{{- define "get-key-data" -}}
  {{/* TODO */}}
{{- end -}}