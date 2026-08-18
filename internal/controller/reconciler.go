package controller

import (
	"context"
	"fmt"
	"time"

	volumediatorv1alpha1 "github.com/tarik02-org/volumediator/api/v1alpha1"
	"github.com/tarik02-org/volumediator/internal/domain"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	policyv1 "k8s.io/api/policy/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type Reconciler struct {
	client.Client
	WebhookNamespace string
	WebhookService   string
	ForceDeleteAfter time.Duration
	UnstageTimeout   time.Duration
	RestageTimeout   time.Duration
	TerminalTTL      time.Duration
}

func (r *Reconciler) SetupWithManager(manager ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(manager).
		For(&volumediatorv1alpha1.VolumeRemediation{}).
		Complete(r)
}

func (r *Reconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	remediation := &volumediatorv1alpha1.VolumeRemediation{}
	if err := r.Get(ctx, request.NamespacedName, remediation); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !remediation.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(remediation, domain.RemediationFinalizer) {
			if err := r.release(ctx, remediation); err != nil {
				return ctrl.Result{}, err
			}
			base := remediation.DeepCopy()
			controllerutil.RemoveFinalizer(remediation, domain.RemediationFinalizer)
			if err := r.Patch(ctx, remediation, client.MergeFrom(base)); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(remediation, domain.RemediationFinalizer) {
		base := remediation.DeepCopy()
		controllerutil.AddFinalizer(remediation, domain.RemediationFinalizer)
		if err := r.Patch(ctx, remediation, client.MergeFrom(base)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	releaseToken := remediation.Annotations[domain.ReleaseAnnotation]
	if releaseToken != "" && releaseToken != remediation.Status.LastReleaseToken {
		if err := r.release(ctx, remediation); err != nil {
			return ctrl.Result{}, err
		}
		return r.updateStatus(ctx, remediation, func() {
			now := metav1.Now()
			remediation.Status.Phase = volumediatorv1alpha1.VolumeRemediationPhaseReleased
			remediation.Status.CompletedAt = &now
			remediation.Status.LastReleaseToken = releaseToken
			apimeta.SetStatusCondition(&remediation.Status.Conditions, metav1.Condition{
				Type: domain.ConditionReady, Status: metav1.ConditionFalse, Reason: "ReleasedByOperator",
				Message: "the operator released the volume without completing remediation",
			})
		})
	}

	retryToken := remediation.Annotations[domain.RetryAnnotation]
	if retryToken != "" && retryToken != remediation.Status.LastRetryToken &&
		(remediation.Status.Phase == volumediatorv1alpha1.VolumeRemediationPhaseHolding ||
			remediation.Status.Phase == volumediatorv1alpha1.VolumeRemediationPhaseFailed) {
		return r.updateStatus(ctx, remediation, func() {
			remediation.Status.Phase = volumediatorv1alpha1.VolumeRemediationPhaseQuiescing
			remediation.Status.Attempts++
			remediation.Status.LastRetryToken = retryToken
			remediation.Status.QuiesceStartedAt = ptrTime(metav1.Now())
			remediation.Status.RestageStartedAt = nil
			remediation.Status.UnmountApproved = false
			remediation.Status.HoldAfterUnstage = false
			remediation.Status.Filesystem = nil
			remediation.Status.SourceMount = nil
			apimeta.SetStatusCondition(&remediation.Status.Conditions, metav1.Condition{
				Type: domain.ConditionReady, Status: metav1.ConditionFalse, Reason: "Retrying",
				Message: "the operator requested another remediation attempt",
			})
		})
	}

	if remediation.Status.Phase == volumediatorv1alpha1.VolumeRemediationPhaseSucceeded ||
		remediation.Status.Phase == volumediatorv1alpha1.VolumeRemediationPhaseReleased {
		if remediation.Status.CompletedAt == nil {
			return r.updateStatus(ctx, remediation, func() {
				now := metav1.Now()
				remediation.Status.CompletedAt = &now
			})
		}
		if r.TerminalTTL == 0 {
			return ctrl.Result{}, nil
		}
		if remaining := time.Until(remediation.Status.CompletedAt.Add(r.TerminalTTL)); remaining > 0 {
			return ctrl.Result{RequeueAfter: remaining}, nil
		}
		if remediation.Status.Phase == volumediatorv1alpha1.VolumeRemediationPhaseReleased &&
			(remediation.Status.Filesystem == nil ||
				remediation.Status.Filesystem.State != domain.FilesystemStateClean ||
				remediation.Status.Filesystem.ErrorsCount != 0) {
			pvc := &corev1.PersistentVolumeClaim{}
			err := r.Get(ctx, types.NamespacedName{Namespace: remediation.Namespace, Name: remediation.Spec.PVCRef.Name}, pvc)
			if err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
			if err == nil && pvc.UID == remediation.Spec.PVCRef.UID {
				return ctrl.Result{RequeueAfter: time.Hour}, nil
			}
		}
		if err := r.Delete(ctx, remediation); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	available, err := r.webhookAvailable(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !available {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	switch remediation.Status.Phase {
	case "", volumediatorv1alpha1.VolumeRemediationPhasePending:
		return r.start(ctx, remediation)
	case volumediatorv1alpha1.VolumeRemediationPhaseQuarantining:
		return r.quarantine(ctx, remediation)
	case volumediatorv1alpha1.VolumeRemediationPhaseQuiescing:
		return r.quiesce(ctx, remediation)
	case volumediatorv1alpha1.VolumeRemediationPhaseUnstaging:
		return r.waitForUnstage(ctx, remediation)
	case volumediatorv1alpha1.VolumeRemediationPhaseRestaging:
		return r.waitForRestage(ctx, remediation)
	case volumediatorv1alpha1.VolumeRemediationPhaseHolding,
		volumediatorv1alpha1.VolumeRemediationPhaseFailed:
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	default:
		return r.fail(ctx, remediation, "UnknownPhase", fmt.Sprintf("unknown remediation phase %q", remediation.Status.Phase))
	}
}

func (r *Reconciler) start(ctx context.Context, remediation *volumediatorv1alpha1.VolumeRemediation) (ctrl.Result, error) {
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: remediation.Namespace, Name: remediation.Spec.PVCRef.Name}, pvc); err != nil {
		return r.fail(ctx, remediation, "PVCUnavailable", err.Error())
	}
	if pvc.UID != remediation.Spec.PVCRef.UID || pvc.Spec.VolumeName != remediation.Spec.PVRef.Name {
		return r.fail(ctx, remediation, "PVCChanged", "the PVC no longer refers to the detected volume")
	}
	pv := &corev1.PersistentVolume{}
	if err := r.Get(ctx, types.NamespacedName{Name: remediation.Spec.PVRef.Name}, pv); err != nil {
		return r.fail(ctx, remediation, "PVUnavailable", err.Error())
	}
	if pv.UID != remediation.Spec.PVRef.UID || pv.Spec.CSI == nil ||
		pv.Spec.CSI.Driver != remediation.Spec.Driver || pv.Spec.CSI.VolumeHandle != remediation.Spec.VolumeHandle {
		return r.fail(ctx, remediation, "PVChanged", "the PV no longer matches the detected CSI volume")
	}
	return r.updateStatus(ctx, remediation, func() {
		remediation.Status.Phase = volumediatorv1alpha1.VolumeRemediationPhaseQuarantining
		remediation.Status.Attempts = 1
		remediation.Status.UnmountNode = remediation.Spec.SourceNode
		apimeta.SetStatusCondition(&remediation.Status.Conditions, metav1.Condition{
			Type: domain.ConditionReady, Status: metav1.ConditionFalse, Reason: "RemediationStarted",
			Message: "volume remediation is in progress",
		})
	})
}

func (r *Reconciler) quarantine(ctx context.Context, remediation *volumediatorv1alpha1.VolumeRemediation) (ctrl.Result, error) {
	pvc := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: remediation.Namespace, Name: remediation.Spec.PVCRef.Name}
	if err := r.Get(ctx, key, pvc); err != nil {
		return ctrl.Result{}, err
	}
	if pvc.UID != remediation.Spec.PVCRef.UID {
		return r.fail(ctx, remediation, "PVCChanged", "the PVC was replaced before quarantine")
	}
	if owner := pvc.Annotations[domain.QuarantineAnnotation]; owner != "" && owner != string(remediation.UID) {
		return r.fail(ctx, remediation, "AlreadyQuarantined", "another remediation owns the PVC quarantine")
	}
	base := pvc.DeepCopy()
	if pvc.Annotations == nil {
		pvc.Annotations = map[string]string{}
	}
	pvc.Annotations[domain.QuarantineAnnotation] = string(remediation.UID)
	if err := r.Patch(ctx, pvc, client.MergeFrom(base)); err != nil {
		return ctrl.Result{}, err
	}
	return r.updateStatus(ctx, remediation, func() {
		remediation.Status.Phase = volumediatorv1alpha1.VolumeRemediationPhaseQuiescing
		remediation.Status.QuiesceStartedAt = ptrTime(metav1.Now())
	})
}

func (r *Reconciler) quiesce(ctx context.Context, remediation *volumediatorv1alpha1.VolumeRemediation) (ctrl.Result, error) {
	pods, err := r.consumerPods(ctx, remediation)
	if err != nil {
		return ctrl.Result{}, err
	}
	for i := range pods {
		pod := &pods[i]
		if pod.Spec.NodeName == "" {
			continue
		}
		if len(pod.OwnerReferences) == 0 {
			return r.hold(ctx, remediation, "OwnerlessConsumer", fmt.Sprintf("pod %s has no controller and cannot be recreated", pod.Name))
		}
		if remediation.Status.QuiesceStartedAt != nil && time.Since(remediation.Status.QuiesceStartedAt.Time) >= r.ForceDeleteAfter {
			zero := int64(0)
			if err := r.Delete(ctx, pod, &client.DeleteOptions{GracePeriodSeconds: &zero}); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
			continue
		}
		if pod.DeletionTimestamp.IsZero() {
			eviction := &policyv1.Eviction{ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace}}
			if err := r.SubResource("eviction").Create(ctx, pod, eviction); err != nil &&
				!apierrors.IsNotFound(err) && !apierrors.IsTooManyRequests(err) {
				return ctrl.Result{}, err
			}
		}
	}
	if hasNodeBoundPod(pods) {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	return r.updateStatus(ctx, remediation, func() {
		remediation.Status.Phase = volumediatorv1alpha1.VolumeRemediationPhaseUnstaging
		apimeta.SetStatusCondition(&remediation.Status.Conditions, metav1.Condition{
			Type: domain.ConditionConsumersReady, Status: metav1.ConditionFalse, Reason: "ConsumersQuiesced",
			Message: "all node-bound consumers have stopped",
		})
	})
}

func (r *Reconciler) waitForUnstage(ctx context.Context, remediation *volumediatorv1alpha1.VolumeRemediation) (ctrl.Result, error) {
	pods, err := r.consumerPods(ctx, remediation)
	if err != nil {
		return ctrl.Result{}, err
	}
	if hasNodeBoundPod(pods) {
		return r.updateStatus(ctx, remediation, func() {
			remediation.Status.Phase = volumediatorv1alpha1.VolumeRemediationPhaseQuiescing
		})
	}
	attached, err := r.hasAttachedVolume(ctx, remediation.Spec.PVRef.Name)
	if err != nil {
		return ctrl.Result{}, err
	}
	unstaged := remediation.Status.SourceMount != nil && !remediation.Status.SourceMount.Staged &&
		remediation.Status.QuiesceStartedAt != nil &&
		remediation.Status.SourceMount.ObservedAt.After(remediation.Status.QuiesceStartedAt.Time)
	if !attached && !unstaged && !remediation.Status.UnmountApproved {
		return r.updateStatus(ctx, remediation, func() {
			remediation.Status.UnmountApproved = true
		})
	}
	if !unstaged || attached {
		if remediation.Status.QuiesceStartedAt != nil && time.Since(remediation.Status.QuiesceStartedAt.Time) >= r.UnstageTimeout {
			return r.fail(ctx, remediation, "UnstageTimeout", "CSI did not completely unstage and detach the volume")
		}
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	if remediation.Status.HoldAfterUnstage {
		return r.updateStatus(ctx, remediation, func() {
			remediation.Status.Phase = volumediatorv1alpha1.VolumeRemediationPhaseHolding
			remediation.Status.UnmountApproved = false
		})
	}
	return r.updateStatus(ctx, remediation, func() {
		remediation.Status.Phase = volumediatorv1alpha1.VolumeRemediationPhaseRestaging
		remediation.Status.RestageStartedAt = ptrTime(metav1.Now())
		remediation.Status.UnmountApproved = false
		remediation.Status.Filesystem = nil
	})
}

func (r *Reconciler) waitForRestage(ctx context.Context, remediation *volumediatorv1alpha1.VolumeRemediation) (ctrl.Result, error) {
	if err := r.removeGate(ctx, remediation); err != nil {
		return ctrl.Result{}, err
	}
	observation := remediation.Status.Filesystem
	fresh := observation != nil && observation.Staged && remediation.Status.RestageStartedAt != nil &&
		observation.ObservedAt.After(remediation.Status.RestageStartedAt.Time)
	if fresh {
		if observation.State != domain.FilesystemStateClean || observation.ErrorsCount != 0 {
			return r.prepareHold(ctx, remediation, "FilesystemStillDamaged", fmt.Sprintf("filesystem restaged as %q with %d errors", observation.State, observation.ErrorsCount))
		}
		if err := r.release(ctx, remediation); err != nil {
			return ctrl.Result{}, err
		}
		return r.updateStatus(ctx, remediation, func() {
			now := metav1.Now()
			remediation.Status.Phase = volumediatorv1alpha1.VolumeRemediationPhaseSucceeded
			remediation.Status.CompletedAt = &now
			apimeta.SetStatusCondition(&remediation.Status.Conditions, metav1.Condition{
				Type: domain.ConditionReady, Status: metav1.ConditionTrue, Reason: "FilesystemClean",
				Message: "the filesystem mounted cleanly after CSI restage",
			})
			apimeta.SetStatusCondition(&remediation.Status.Conditions, metav1.Condition{
				Type: domain.ConditionConsumersReady, Status: metav1.ConditionTrue, Reason: "QuarantineReleased",
				Message: "consumer scheduling gates were released",
			})
		})
	}
	if remediation.Status.RestageStartedAt != nil && time.Since(remediation.Status.RestageStartedAt.Time) >= r.RestageTimeout {
		return r.prepareHold(ctx, remediation, "RestageTimeout", "no clean mounted filesystem was observed after releasing consumers")
	}
	return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
}

func (r *Reconciler) consumerPods(ctx context.Context, remediation *volumediatorv1alpha1.VolumeRemediation) ([]corev1.Pod, error) {
	var list corev1.PodList
	if err := r.List(ctx, &list, client.InNamespace(remediation.Namespace)); err != nil {
		return nil, err
	}
	pods := make([]corev1.Pod, 0)
	for i := range list.Items {
		for _, volume := range list.Items[i].Spec.Volumes {
			if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == remediation.Spec.PVCRef.Name {
				pods = append(pods, list.Items[i])
				break
			}
		}
	}
	return pods, nil
}

func hasNodeBoundPod(pods []corev1.Pod) bool {
	for i := range pods {
		if pods[i].Spec.NodeName != "" {
			return true
		}
	}
	return false
}

func (r *Reconciler) hasAttachedVolume(ctx context.Context, pvName string) (bool, error) {
	var attachments storagev1.VolumeAttachmentList
	if err := r.List(ctx, &attachments); err != nil {
		return false, err
	}
	for i := range attachments.Items {
		attachment := &attachments.Items[i]
		if attachment.Spec.Source.PersistentVolumeName != nil && *attachment.Spec.Source.PersistentVolumeName == pvName && attachment.Status.Attached {
			return true, nil
		}
	}
	return false, nil
}

func (r *Reconciler) removeGate(ctx context.Context, remediation *volumediatorv1alpha1.VolumeRemediation) error {
	pods, err := r.consumerPods(ctx, remediation)
	if err != nil {
		return err
	}
	gateName := domain.GateName(string(remediation.UID))
	for i := range pods {
		pod := &pods[i]
		base := pod.DeepCopy()
		gates := make([]corev1.PodSchedulingGate, 0, len(pod.Spec.SchedulingGates))
		for _, gate := range pod.Spec.SchedulingGates {
			if gate.Name != gateName {
				gates = append(gates, gate)
			}
		}
		if len(gates) == len(pod.Spec.SchedulingGates) {
			continue
		}
		pod.Spec.SchedulingGates = gates
		if err := r.Patch(ctx, pod, client.MergeFrom(base)); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (r *Reconciler) release(ctx context.Context, remediation *volumediatorv1alpha1.VolumeRemediation) error {
	pvc := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: remediation.Namespace, Name: remediation.Spec.PVCRef.Name}
	if err := r.Get(ctx, key, pvc); err != nil && !apierrors.IsNotFound(err) {
		return err
	} else if err == nil && pvc.UID == remediation.Spec.PVCRef.UID && pvc.Annotations[domain.QuarantineAnnotation] == string(remediation.UID) {
		base := pvc.DeepCopy()
		delete(pvc.Annotations, domain.QuarantineAnnotation)
		if err := r.Patch(ctx, pvc, client.MergeFrom(base)); err != nil {
			return err
		}
	}
	return r.removeGate(ctx, remediation)
}

func (r *Reconciler) webhookAvailable(ctx context.Context) (bool, error) {
	var slices discoveryv1.EndpointSliceList
	if err := r.List(ctx, &slices,
		client.InNamespace(r.WebhookNamespace),
		client.MatchingLabels{discoveryv1.LabelServiceName: r.WebhookService}); err != nil {
		return false, err
	}
	for _, slice := range slices.Items {
		for _, endpoint := range slice.Endpoints {
			if endpoint.Conditions.Ready == nil || *endpoint.Conditions.Ready {
				return true, nil
			}
		}
	}
	return false, nil
}

func (r *Reconciler) hold(ctx context.Context, remediation *volumediatorv1alpha1.VolumeRemediation, reason, message string) (ctrl.Result, error) {
	return r.updateStatus(ctx, remediation, func() {
		remediation.Status.Phase = volumediatorv1alpha1.VolumeRemediationPhaseHolding
		apimeta.SetStatusCondition(&remediation.Status.Conditions, metav1.Condition{
			Type: domain.ConditionReady, Status: metav1.ConditionFalse, Reason: reason, Message: message,
		})
	})
}

func (r *Reconciler) fail(ctx context.Context, remediation *volumediatorv1alpha1.VolumeRemediation, reason, message string) (ctrl.Result, error) {
	return r.updateStatus(ctx, remediation, func() {
		remediation.Status.Phase = volumediatorv1alpha1.VolumeRemediationPhaseFailed
		apimeta.SetStatusCondition(&remediation.Status.Conditions, metav1.Condition{
			Type: domain.ConditionReady, Status: metav1.ConditionFalse, Reason: reason, Message: message,
		})
	})
}

func (r *Reconciler) prepareHold(ctx context.Context, remediation *volumediatorv1alpha1.VolumeRemediation, reason, message string) (ctrl.Result, error) {
	unmountNode := remediation.Status.UnmountNode
	if remediation.Status.Filesystem != nil && remediation.Status.Filesystem.Node != "" {
		unmountNode = remediation.Status.Filesystem.Node
	} else {
		pods, err := r.consumerPods(ctx, remediation)
		if err != nil {
			return ctrl.Result{}, err
		}
		for i := range pods {
			if pods[i].Spec.NodeName != "" {
				unmountNode = pods[i].Spec.NodeName
				break
			}
		}
	}
	return r.updateStatus(ctx, remediation, func() {
		remediation.Status.Phase = volumediatorv1alpha1.VolumeRemediationPhaseQuiescing
		remediation.Status.QuiesceStartedAt = ptrTime(metav1.Now())
		remediation.Status.RestageStartedAt = nil
		remediation.Status.UnmountApproved = false
		remediation.Status.UnmountNode = unmountNode
		remediation.Status.HoldAfterUnstage = true
		remediation.Status.SourceMount = nil
		apimeta.SetStatusCondition(&remediation.Status.Conditions, metav1.Condition{
			Type: domain.ConditionReady, Status: metav1.ConditionFalse, Reason: reason, Message: message,
		})
	})
}

func (r *Reconciler) updateStatus(ctx context.Context, remediation *volumediatorv1alpha1.VolumeRemediation, update func()) (ctrl.Result, error) {
	base := remediation.DeepCopy()
	update()
	if err := r.Status().Patch(ctx, remediation, client.MergeFrom(base)); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

func ptrTime(value metav1.Time) *metav1.Time {
	return &value
}
