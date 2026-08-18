package domain

const (
	Group                    = "volumediator.tarik02.me"
	EnabledAnnotation        = Group + "/enabled"
	QuarantineAnnotation     = Group + "/quarantine"
	RetryAnnotation          = Group + "/retry"
	ReleaseAnnotation        = Group + "/release"
	PVCUIDLabel              = Group + "/pvc-uid"
	IncidentIDLabel          = Group + "/incident-id"
	ManagedByLabel           = "app.kubernetes.io/managed-by"
	ManagedByValue           = "volumediator"
	RemediationFinalizer     = Group + "/cleanup"
	Ext4ErrorsDetector       = "ext4-errors"
	ConditionReady           = "Ready"
	ConditionConsumersReady  = "ConsumersReady"
	FilesystemStateClean     = "clean"
	FilesystemStateWithError = "clean with errors"
)

func GateName(uid string) string {
	return Group + "/rem-" + uid
}
