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
{{- if and .Values.compatibility.enforceImmutableImages (not .Values.image.digest) -}}
{{- fail "image.digest is required; CSI release manifests must use an immutable digest" -}}
{{- end -}}
{{- if .Values.image.digest -}}
{{- printf "%s:%s@%s" .Values.image.repository .Values.image.tag .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag -}}
{{- end -}}
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
