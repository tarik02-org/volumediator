package node

import (
	"bufio"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	volumediatorv1alpha1 "github.com/tarik02-org/volumediator/api/v1alpha1"
	"github.com/tarik02-org/volumediator/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Runner struct {
	Client                client.Client
	Log                   logr.Logger
	NodeName              string
	HostRoot              string
	ScanInterval          time.Duration
	Tune2fsPath           string
	UmountPath            string
	AllowedStorageClasses map[string]struct{}
	BlockedStorageClasses map[string]struct{}
	ErrorStates           map[string]struct{}
}

type mount struct {
	target     string
	source     string
	deviceName string
	deviceID   string
	fsType     string
}

func (r *Runner) Run(ctx context.Context) error {
	if err := r.scan(ctx); err != nil {
		r.Log.Error(err, "initial scan failed")
	}

	ticker := time.NewTicker(r.ScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := r.scan(ctx); err != nil {
				r.Log.Error(err, "scan failed")
			}
		}
	}
}

func (r *Runner) scan(ctx context.Context) error {
	mounts, podMounts, err := r.readMounts()
	if err != nil {
		return err
	}

	var remediations volumediatorv1alpha1.VolumeRemediationList
	if err := r.Client.List(ctx, &remediations); err != nil {
		return fmt.Errorf("list remediations: %w", err)
	}

	activeByPVCUID := make(map[types.UID]*volumediatorv1alpha1.VolumeRemediation)
	releasedByPVCUID := make(map[types.UID]*volumediatorv1alpha1.VolumeRemediation)
	for i := range remediations.Items {
		remediation := &remediations.Items[i]
		if remediation.Status.Phase == volumediatorv1alpha1.VolumeRemediationPhaseReleased {
			previous := releasedByPVCUID[remediation.Spec.PVCRef.UID]
			if previous == nil || remediation.CreationTimestamp.After(previous.CreationTimestamp.Time) {
				releasedByPVCUID[remediation.Spec.PVCRef.UID] = remediation
			}
			continue
		}
		if remediation.Status.Phase == volumediatorv1alpha1.VolumeRemediationPhaseSucceeded {
			continue
		}
		activeByPVCUID[remediation.Spec.PVCRef.UID] = remediation
	}

	var pvs corev1.PersistentVolumeList
	if err := r.Client.List(ctx, &pvs); err != nil {
		return fmt.Errorf("list persistent volumes: %w", err)
	}

	for i := range pvs.Items {
		pv := &pvs.Items[i]
		if pv.Spec.CSI == nil || pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID == "" {
			continue
		}

		hash := sha256.Sum256([]byte(pv.Spec.CSI.VolumeHandle))
		target := fmt.Sprintf("/var/lib/kubelet/plugins/kubernetes.io/csi/%s/%x/globalmount", pv.Spec.CSI.Driver, hash)
		mounted, isMounted := mounts[target]
		active := activeByPVCUID[pv.Spec.ClaimRef.UID]
		released := releasedByPVCUID[pv.Spec.ClaimRef.UID]
		unmountNode := ""
		if active != nil {
			unmountNode = active.Status.UnmountNode
			if unmountNode == "" {
				unmountNode = active.Spec.SourceNode
			}
		}

		if active != nil && unmountNode == r.NodeName && isMounted &&
			active.Status.Phase == volumediatorv1alpha1.VolumeRemediationPhaseUnstaging && active.Status.UnmountApproved {
			if podMounts[mounted.deviceID] {
				r.Log.Info("waiting for local pod mount before approved unmount", "remediation", client.ObjectKeyFromObject(active), "device", mounted.source)
			} else if err := r.unmount(ctx, target); err != nil {
				r.Log.Error(err, "approved CSI global unmount failed", "remediation", client.ObjectKeyFromObject(active), "target", target)
			} else {
				r.Log.Info("unmounted approved CSI global mount", "remediation", client.ObjectKeyFromObject(active), "target", target)
			}
			continue
		}

		if active != nil && unmountNode == r.NodeName && !isMounted {
			base := active.DeepCopy()
			active.Status.SourceMount = &volumediatorv1alpha1.SourceMountObservation{
				Staged:     false,
				ObservedAt: metav1.Now(),
			}
			if err := r.Client.Status().Patch(ctx, active, client.MergeFrom(base)); err != nil && !apierrors.IsConflict(err) {
				r.Log.Error(err, "report source unstage", "remediation", client.ObjectKeyFromObject(active))
			}
		}

		if !isMounted || mounted.fsType != "ext4" {
			continue
		}

		errorsCount, err := r.readErrorsCount(mounted.deviceName)
		if err != nil {
			r.Log.Error(err, "read ext4 errors", "pv", pv.Name, "device", mounted.deviceName)
			continue
		}
		if errorsCount == 0 && active == nil && released == nil {
			continue
		}

		state, err := r.readFilesystemState(ctx, mounted.source)
		if err != nil {
			r.Log.Error(err, "read ext4 superblock", "pv", pv.Name, "device", mounted.source)
			continue
		}

		if active != nil {
			base := active.DeepCopy()
			now := metav1.Now()
			active.Status.Filesystem = &volumediatorv1alpha1.FilesystemObservation{
				Node:           r.NodeName,
				FilesystemType: "ext4",
				State:          state,
				ErrorsCount:    errorsCount,
				Staged:         true,
				ObservedAt:     now,
			}
			if unmountNode == r.NodeName {
				active.Status.SourceMount = &volumediatorv1alpha1.SourceMountObservation{
					Staged:     true,
					ObservedAt: now,
				}
			}
			if err := r.Client.Status().Patch(ctx, active, client.MergeFrom(base)); err != nil && !apierrors.IsConflict(err) {
				r.Log.Error(err, "report filesystem observation", "remediation", client.ObjectKeyFromObject(active))
			}
			continue
		}

		if released != nil {
			if state == domain.FilesystemStateClean && errorsCount == 0 {
				if released.Status.Filesystem == nil || released.Status.Filesystem.State != domain.FilesystemStateClean || released.Status.Filesystem.ErrorsCount != 0 {
					base := released.DeepCopy()
					released.Status.Filesystem = &volumediatorv1alpha1.FilesystemObservation{
						Node: r.NodeName, FilesystemType: "ext4", State: state, ErrorsCount: 0,
						Staged: true, ObservedAt: metav1.Now(),
					}
					if err := r.Client.Status().Patch(ctx, released, client.MergeFrom(base)); err != nil && !apierrors.IsConflict(err) {
						r.Log.Error(err, "record clean state after release", "remediation", client.ObjectKeyFromObject(released))
					}
				}
				continue
			}
			if released.Status.Filesystem == nil || released.Status.Filesystem.State != domain.FilesystemStateClean || released.Status.Filesystem.ErrorsCount != 0 {
				continue
			}
		}

		if _, damaged := r.ErrorStates[state]; !damaged || errorsCount == 0 {
			continue
		}

		pvc := &corev1.PersistentVolumeClaim{}
		pvcKey := client.ObjectKey{Namespace: pv.Spec.ClaimRef.Namespace, Name: pv.Spec.ClaimRef.Name}
		if err := r.Client.Get(ctx, pvcKey, pvc); err != nil {
			r.Log.Error(err, "get persistent volume claim", "pvc", pvcKey)
			continue
		}
		if pvc.UID != pv.Spec.ClaimRef.UID || !r.isEligible(pvc) {
			continue
		}

		namePrefix := pvc.Name
		if len(namePrefix) > 50 {
			namePrefix = namePrefix[:50]
		}
		remediation := &volumediatorv1alpha1.VolumeRemediation{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: namePrefix + "-",
				Namespace:    pvc.Namespace,
				Labels: map[string]string{
					domain.PVCUIDLabel:    string(pvc.UID),
					domain.ManagedByLabel: domain.ManagedByValue,
				},
			},
			Spec: volumediatorv1alpha1.VolumeRemediationSpec{
				PVCRef:       volumediatorv1alpha1.ResourceReference{Name: pvc.Name, UID: pvc.UID},
				PVRef:        volumediatorv1alpha1.ResourceReference{Name: pv.Name, UID: pv.UID},
				SourceNode:   r.NodeName,
				Driver:       pv.Spec.CSI.Driver,
				VolumeHandle: pv.Spec.CSI.VolumeHandle,
				Detection: volumediatorv1alpha1.Detection{
					Rule:           domain.Ext4ErrorsDetector,
					FilesystemType: "ext4",
					State:          state,
					ErrorsCount:    errorsCount,
					ObservedAt:     metav1.Now(),
				},
			},
		}
		if err := r.Client.Create(ctx, remediation); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				r.Log.Error(err, "create remediation", "pvc", pvcKey)
			}
			continue
		}
		r.Log.Info("created remediation", "remediation", client.ObjectKeyFromObject(remediation), "pv", pv.Name, "state", state, "errors", errorsCount)
	}

	return nil
}

func (r *Runner) isEligible(pvc *corev1.PersistentVolumeClaim) bool {
	if enabled, present := pvc.Annotations[domain.EnabledAnnotation]; present {
		return enabled == "true"
	}
	if pvc.Spec.StorageClassName == nil {
		return false
	}
	if _, blocked := r.BlockedStorageClasses[*pvc.Spec.StorageClassName]; blocked {
		return false
	}
	_, allowed := r.AllowedStorageClasses[*pvc.Spec.StorageClassName]
	return allowed
}

func (r *Runner) readMounts() (map[string]mount, map[string]bool, error) {
	file, err := os.Open(filepath.Join(r.HostRoot, "proc/1/mountinfo"))
	if err != nil {
		return nil, nil, fmt.Errorf("open host mountinfo: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			r.Log.Error(err, "close host mountinfo")
		}
	}()

	mounts := make(map[string]mount)
	podMounts := make(map[string]bool)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		separator := -1
		for i, field := range fields {
			if field == "-" {
				separator = i
				break
			}
		}
		if separator < 0 || separator+2 >= len(fields) {
			continue
		}
		target := strings.ReplaceAll(fields[4], `\040`, " ")
		if strings.Contains(target, "/pods/") && strings.Contains(target, "/volumes/kubernetes.io~csi/") {
			podMounts[fields[2]] = true
		}
		if !strings.Contains(target, "/plugins/kubernetes.io/csi/") || !strings.HasSuffix(target, "/globalmount") {
			continue
		}
		deviceLink := filepath.Join(r.HostRoot, "sys/dev/block", fields[2])
		resolved, err := os.Readlink(deviceLink)
		if err != nil {
			return nil, nil, fmt.Errorf("resolve device %s: %w", fields[2], err)
		}
		mounts[target] = mount{
			target:     target,
			source:     fields[separator+2],
			deviceName: filepath.Base(resolved),
			deviceID:   fields[2],
			fsType:     fields[separator+1],
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, fmt.Errorf("read host mountinfo: %w", err)
	}
	return mounts, podMounts, nil
}

func (r *Runner) unmount(ctx context.Context, target string) error {
	commandCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	output, err := exec.CommandContext(commandCtx, r.UmountPath, target).CombinedOutput()
	if err != nil {
		return fmt.Errorf("umount %s: %w: %s", target, err, strings.TrimSpace(string(output)))
	}
	return nil
}

func (r *Runner) readErrorsCount(deviceName string) (int64, error) {
	value, err := os.ReadFile(filepath.Join(r.HostRoot, "sys/fs/ext4", deviceName, "errors_count"))
	if err != nil {
		return 0, err
	}
	errorsCount, err := strconv.ParseInt(strings.TrimSpace(string(value)), 10, 64)
	if err != nil {
		return 0, err
	}
	return errorsCount, nil
}

func (r *Runner) readFilesystemState(ctx context.Context, source string) (string, error) {
	if !strings.HasPrefix(source, "/dev/") {
		return "", fmt.Errorf("unsupported ext4 source %q", source)
	}
	commandCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	output, err := exec.CommandContext(commandCtx, r.Tune2fsPath, "-l", filepath.Join(r.HostRoot, source)).Output()
	if err != nil {
		if errors.Is(commandCtx.Err(), context.DeadlineExceeded) {
			return "", commandCtx.Err()
		}
		return "", err
	}
	for _, line := range strings.Split(string(output), "\n") {
		if state, found := strings.CutPrefix(line, "Filesystem state:"); found {
			return strings.TrimSpace(state), nil
		}
	}
	return "", fmt.Errorf("filesystem state not found")
}
