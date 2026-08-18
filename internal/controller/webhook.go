package controller

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/tarik02-org/volumediator/internal/domain"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

type PodMutator struct {
	Client  client.Client
	Decoder admission.Decoder
}

func (m *PodMutator) Handle(ctx context.Context, request admission.Request) admission.Response {
	if request.Operation != admissionv1.Create {
		return admission.Allowed("only Pod creation can add scheduling gates")
	}

	pod := &corev1.Pod{}
	if err := m.Decoder.Decode(request, pod); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	gates := make(map[string]struct{}, len(pod.Spec.SchedulingGates))
	for _, gate := range pod.Spec.SchedulingGates {
		gates[gate.Name] = struct{}{}
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		pvc := &corev1.PersistentVolumeClaim{}
		err := m.Client.Get(ctx, types.NamespacedName{Namespace: request.Namespace, Name: volume.PersistentVolumeClaim.ClaimName}, pvc)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return admission.Errored(http.StatusServiceUnavailable, err)
		}
		uid := pvc.Annotations[domain.QuarantineAnnotation]
		if uid == "" {
			continue
		}
		gateName := domain.GateName(uid)
		if _, exists := gates[gateName]; exists {
			continue
		}
		pod.Spec.SchedulingGates = append(pod.Spec.SchedulingGates, corev1.PodSchedulingGate{Name: gateName})
		gates[gateName] = struct{}{}
	}

	marshaled, err := json.Marshal(pod)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(request.Object.Raw, marshaled)
}
