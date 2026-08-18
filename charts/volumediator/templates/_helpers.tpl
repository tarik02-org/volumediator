{{- define "volumediator.name" -}}
volumediator
{{- end }}

{{- define "volumediator.labels" -}}
app.kubernetes.io/name: {{ include "volumediator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}
