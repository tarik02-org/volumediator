# Volumediator

Volumediator recovers damaged filesystems behind Kubernetes PVCs without
draining their nodes or deleting storage objects.

It runs in two modes from one image:

- `volumediator node` detects explicitly configured filesystem errors on its
  node and reports `VolumeRemediation` resources.
- `volumediator controller` quarantines the affected PVC, gates replacement
  Pods, waits for complete CSI unstage, and releases the Pods so CSI can repair
  the filesystem during restage.

The first detector is `ext4-errors`. No detector is enabled implicitly.

The controller can approve an exact CSI global unmount when a driver reports a
successful unstage but leaves the mount behind. Approval requires no
node-bound consumer and no attached `VolumeAttachment`. The source node checks
for kubelet Pod bind mounts of the same device before it runs `umount`.

## Install

The chart requires cert-manager. A policy must explicitly name both eligible
StorageClasses and detector states:

```sh
helm upgrade --install volumediator \
  oci://ghcr.io/tarik02-org/charts/volumediator \
  --version 0.1.0 \
  --namespace volumediator-system --create-namespace \
  --set policy.allowedStorageClasses='{truenas-iscsi}' \
  --set policy.ext4ErrorStates='{clean with errors}'
```

A PVC can override the StorageClass policy:

```sh
kubectl annotate pvc data volumediator.tarik02.me/enabled=true
kubectl annotate pvc data volumediator.tarik02.me/enabled=false
```

Volumediator retains quarantine after a failed attempt. Set a new token to
retry the same incident, or release it manually:

```sh
kubectl annotate volumeremediation <name> volumediator.tarik02.me/retry="$(date +%s)"
kubectl annotate volumeremediation <name> volumediator.tarik02.me/release="$(date +%s)"
```

The release control removes quarantine without claiming that the filesystem is
clean. Use it only after checking the volume yourself.

Terminal remediations are retained for seven days by default. Released
remediations remain while their PVC still exposes the damaged filesystem.

See [AI.md](AI.md) for the project's AI assistance policy.
