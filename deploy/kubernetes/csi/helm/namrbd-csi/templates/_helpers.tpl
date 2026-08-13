{{- define "namrbd-csi.name" -}}
namrbd-csi
{{- end -}}

{{- define "namrbd-csi.namespace" -}}
{{- default .Release.Namespace .Values.namespaceOverride -}}
{{- end -}}

{{- define "namrbd-csi.labels" -}}
app.kubernetes.io/name: namrbd-csi
app.kubernetes.io/instance: {{ .Release.Name | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
{{- end -}}

{{- define "namrbd-csi.driverImage" -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag -}}
{{- end -}}

{{- define "namrbd-csi.adminEndpoints" -}}
{{- if kindIs "slice" .Values.config.adminEndpoints -}}
{{- join "," .Values.config.adminEndpoints -}}
{{- else -}}
{{- .Values.config.adminEndpoints -}}
{{- end -}}
{{- end -}}

{{- define "namrbd-csi.credentialsSecretName" -}}
{{- .Values.credentials.existingSecret -}}
{{- end -}}
