package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type VolumeRemediationPhase string

const (
	VolumeRemediationPhasePending      VolumeRemediationPhase = "Pending"
	VolumeRemediationPhaseQuarantining VolumeRemediationPhase = "Quarantining"
	VolumeRemediationPhaseQuiescing    VolumeRemediationPhase = "Quiescing"
	VolumeRemediationPhaseUnstaging    VolumeRemediationPhase = "Unstaging"
	VolumeRemediationPhaseRestaging    VolumeRemediationPhase = "Restaging"
	VolumeRemediationPhaseHolding      VolumeRemediationPhase = "Holding"
	VolumeRemediationPhaseSucceeded    VolumeRemediationPhase = "Succeeded"
	VolumeRemediationPhaseFailed       VolumeRemediationPhase = "Failed"
	VolumeRemediationPhaseReleased     VolumeRemediationPhase = "Released"
)

type ResourceReference struct {
	Name string    `json:"name"`
	UID  types.UID `json:"uid"`
}

type Detection struct {
	Rule           string      `json:"rule"`
	FilesystemType string      `json:"filesystemType"`
	State          string      `json:"state"`
	ErrorsCount    int64       `json:"errorsCount"`
	ObservedAt     metav1.Time `json:"observedAt"`
}

type VolumeRemediationSpec struct {
	PVCRef       ResourceReference `json:"pvcRef"`
	PVRef        ResourceReference `json:"pvRef"`
	SourceNode   string            `json:"sourceNode"`
	Driver       string            `json:"driver"`
	VolumeHandle string            `json:"volumeHandle"`
	Detection    Detection         `json:"detection"`
}

type FilesystemObservation struct {
	Node           string      `json:"node"`
	FilesystemType string      `json:"filesystemType"`
	State          string      `json:"state"`
	ErrorsCount    int64       `json:"errorsCount"`
	Staged         bool        `json:"staged"`
	ObservedAt     metav1.Time `json:"observedAt"`
}

type SourceMountObservation struct {
	Staged     bool        `json:"staged"`
	ObservedAt metav1.Time `json:"observedAt"`
}

type VolumeRemediationStatus struct {
	Phase            VolumeRemediationPhase  `json:"phase,omitempty"`
	Attempts         int32                   `json:"attempts,omitempty"`
	QuiesceStartedAt *metav1.Time            `json:"quiesceStartedAt,omitempty"`
	RestageStartedAt *metav1.Time            `json:"restageStartedAt,omitempty"`
	UnmountApproved  bool                    `json:"unmountApproved,omitempty"`
	UnmountNode      string                  `json:"unmountNode,omitempty"`
	HoldAfterUnstage bool                    `json:"holdAfterUnstage,omitempty"`
	Filesystem       *FilesystemObservation  `json:"filesystem,omitempty"`
	SourceMount      *SourceMountObservation `json:"sourceMount,omitempty"`
	LastRetryToken   string                  `json:"lastRetryToken,omitempty"`
	LastReleaseToken string                  `json:"lastReleaseToken,omitempty"`
	Conditions       []metav1.Condition      `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=vmr
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="PVC",type=string,JSONPath=`.spec.pvcRef.name`
// +kubebuilder:printcolumn:name="Node",type=string,JSONPath=`.spec.sourceNode`
// +kubebuilder:printcolumn:name="Attempts",type=integer,JSONPath=`.status.attempts`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type VolumeRemediation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VolumeRemediationSpec   `json:"spec"`
	Status VolumeRemediationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type VolumeRemediationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VolumeRemediation `json:"items"`
}

func init() {
	SchemeBuilder.Register(&VolumeRemediation{}, &VolumeRemediationList{})
}
