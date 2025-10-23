(ref-csi)=
# LXD CSI driver reference

This document contains reference information for the LXD CSI driver CLI, including available Helm chart values and its versioning information.

(ref-csi-cli)=
## CLI

The `lxd-csi-driver` provides CSI controller and node server functionality.
You can configure runtime options using these flags:

Flag               | Default                 | Description
-------------------|-------------------------|------------
`--driver-name`    | `lxd.csi.canonical.com` | CSI driver name
`--endpoint`       | `unix:///tmp/csi.sock`  | Internal CSI Unix socket path
`--devLXDEndpoint` | `unix:///dev/lxd/sock`  | DevLXD Unix socket path
`--nodeID`         | `""`                    | Kubernetes node ID
`--controller`     | `false`                 | Run as controller server
`--version`        |                         | Print version and exit

(ref-csi-helm)=
## Helm chart

The LXD CSI driver Helm chart is available as an [OCI image](https://ghcr.io/canonical/charts/lxd-csi-driver).
The source of the Helm chart can be found in the [LXD CSI driver repository](https://github.com/canonical/lxd-csi-driver/tree/main/charts).

The table below contains configurable Helm chart values with their default values and descriptions.

Key                                                | Type   | Default                                                 | Description
---------------------------------------------------|--------|---------------------------------------------------------|------------
`driver.image.repository`                          | string | `ghcr.io/canonical/lxd-csi-driver`                      | LXD CSI image
`driver.image.tag`                                 | string | Chart version                                           | LXD CSI image tag
`driver.image.pullPolicy`                          | string | `IfNotPresent`                                          | LXD CSI image pull policy
`driver.imagePullSecrets`                          | list   | `[]`                                                    | LXD CSI image pull secrets
`driver.tokenSecretName`                           | string | `lxd-csi-secret`                                        | Name of the secret containing DevLXD bearer token in `token` field
`driver.fsGroupPolicy`                             | string | `File`                                                  | Controls Kubernetes `fsGroup` behavior for the driver (`None`, `ReadWriteOnceWithFSType`, `File`)
`driver.volumeNamePrefix`                          | string | `csi`                                                   | Prefix used for LXD volume names. Resulting volume name is in format `<prefix>-<uuid>`.
`rbac.create`                                      | bool   | `true`                                                  | Create RBAC resources allowing LXD CSI access to relevant Kubernetes objects
`controller.name`                                  | string | `lxd-csi-controller`                                    | Controller Deployment name
`controller.replicas`                              | int    | `1`                                                     | Controller Deployment replicas. When deployed in multiple replicas, the preferred affinity is configured in an attempt to distribute Pods across multiple nodes.
`controller.strategy.type`                         | string | `RollingUpdate`                                         | Controller Deployment update strategy (`RollingUpdate`, `Recreate`)
`controller.strategy.rollingUpdate.maxUnavailable` | string | `50%`                                                   | Max unavailable Pods during update
`controller.priorityClassName`                     | string | `system-cluster-critical`                               | Controller Pod scheduling priority
`controller.serviceAccount.create`                 | bool   | `true`                                                  | Whether to create required service account for controller server
`controller.serviceAccount.name`                   | string | `""` (Equal to Controller server name if empty)         | Custom service account name
`controller.runOnControlPlaneOnly`                 | bool   | `true`                                                  | Whether to run controller only on control plane nodes. This ensures appropriate node affinity and tolerations are configured. If node affinity is manually set, this option is disabled.
`controller.nodeSelector`                          | object | `{}`                                                    | Node selector for controller Pods
`controller.tolerations`                           | list   | `[]`                                                    | Controller Pod tolerations
`controller.affinity`                              | object | `{}`                                                    | Controller Pod affinity
`controller.annotations`                           | object | `{}`                                                    | Controller Deployment annotations
`controller.podAnnotations`                        | object | `{}`                                                    | Controller Pod annotations
`controller.resources`                             | object | `{}`                                                    | Controller resource limits and requests
`controller.csiProvisioner.image.repository`       | string | `registry.k8s.io/sig-storage/csi-provisioner`           | CSI provisioner image
`controller.csiProvisioner.image.tag`              | string | Chart release dependent                                 | CSI provisioner image tag
`controller.csiProvisioner.image.pullPolicy`       | string | `IfNotPresent`                                          | CSI provisioner pull policy
`controller.csiProvisioner.resources`              | object | `{}`                                                    | CSI provisioner resource limits and requests
`controller.csiAttacher.image.repository`          | string | `registry.k8s.io/sig-storage/csi-attacher`              | CSI attacher image
`controller.csiAttacher.image.tag`                 | string | Chart release dependent                                 | CSI attacher image tag
`controller.csiAttacher.image.pullPolicy`          | string | `IfNotPresent`                                          | CSI attacher image pull policy
`controller.csiAttacher.resources`                 | object | `{}`                                                    | CSI attacher resource limits and requests
`controller.csiLivenessProbe.image.repository`     | string | `registry.k8s.io/sig-storage/livenessprobe`             | CSI liveness probe image
`controller.csiLivenessProbe.image.tag`            | string | Chart release dependent                                 | CSI liveness probe image tag
`controller.csiLivenessProbe.image.pullPolicy`     | string | `IfNotPresent`                                          | CSI liveness probe image pull policy
`controller.csiLivenessProbe.resources`            | object | `{}`                                                    | CSI liveness probe resource limits and requests
`node.name`                                        | string | `lxd-csi-node`                                          | Node DaemonSet name
`node.strategy.type`                               | string | `RollingUpdate`                                         | Node DaemonSet update strategy (`RollingUpdate`, `OnDelete`)
`node.strategy.rollingUpdate.maxUnavailable`       | string | `1`                                                     | Max unavailable Pods during update
`node.priorityClassName`                           | string | `system-node-critical`                                  | Node Pod scheduling priority
`node.serviceAccount.create`                       | bool   | `true`                                                  | Whether to create required service account for node server
`node.serviceAccount.name`                         | string | `""` (Equal to Node server name if empty)               | Custom service account name
`node.nodeSelector`                                | object | `{}`                                                    | Node selector for node server Pods
`node.tolerations`                                 | list   | `[]`                                                    | Node Pod tolerations
`node.affinity`                                    | object | `{}`                                                    | Node Pod affinity
`node.annotations`                                 | object | `{}`                                                    | Node DaemonSet annotations
`node.podAnnotations`                              | object | `{}`                                                    | Node Pod annotations
`node.resources`                                   | object | `{}`                                                    | Node server resource limits and requests
`node.nodeDriverRegistrar.image.repository`        | string | `registry.k8s.io/sig-storage/csi-node-driver-registrar` | Node driver registrar image
`node.nodeDriverRegistrar.image.tag`               | string | Chart release dependent                                 | Node driver registrar image tag
`node.nodeDriverRegistrar.image.pullPolicy`        | string | `IfNotPresent`                                          | Node driver registrar image pull policy
`node.nodeDriverRegistrar.resources`               | object | `{}`                                                    | Node driver registrar resource limits and requests
`node.csiLivenessProbe.image.repository`           | string | `registry.k8s.io/sig-storage/livenessprobe`             | CSI liveness probe image
`node.csiLivenessProbe.image.tag`                  | string | Chart release dependent                                 | CSI liveness probe image tag
`node.csiLivenessProbe.image.pullPolicy`           | string | `IfNotPresent`                                          | CSI liveness probe image pull policy
`node.csiLivenessProbe.resources`                  | object | `{}`                                                    | CSI liveness probe resource limits and requests
`storageClasses[].create`                          | bool   | `true`                                                  | Create the specified storage class
`storageClasses[].name`                            | string | `""`                                                    | Storage class name
`storageClasses[].storagePool`                     | string | `""`                                                    | Name of the target LXD storage pool
`storageClasses[].volumeBindingMode`               | string | `WaitForFirstConsumer`                                  | Volume binding mode (`Immediate`, `WaitForFirstConsumer`)
`storageClasses[].reclaimPolicy`                   | string | `Delete`                                                | Volume reclaim policy (`Delete`, `Retain`)
`storageClasses[].annotations`                     | object | `{}`                                                    | Additional storage class annotations

(ref-csi-versioning)=
## Versioning

The LXD CSI driver follows Semantic Versioning independently of LXD releases, starting at `0.0.1`.

Tag type | Format                     | Example  | Description
-------- | -------------------------- | -------- | -----------
Patch    | `v<major>.<minor>.<patch>` | `v1.2.3` | Bugfix release. Each patch within the same minor version provides the same set of features.
Minor    | `v<major>.<minor>`         | `v4.5`   | Feature release. Adds new features in a backward-compatible way within the major version. Features may be deprecated, but remain usable.
Major    | `v<major>`                 | `v6`     | Breaking release. May include breaking changes and may raise minimum LXD version.

(ref-csi-versioning-compatibility)=
### Compatibility

For any major driver version `≥1`, the minimum supported LXD version is fixed.
Updates within that major version remain compatible with that LXD version (or newer) until the driver version reaches end-of-life (EOL).

On the other hand, Kubernetes versions receive roughly one year of patch support.
The LXD CSI driver only supports Kubernetes versions that are themselves supported upstream.
When a Kubernetes version reaches end of life, it is no longer supported by the driver.

CSI Version | Min. LXD Version | Min. Kubernetes Version | EOL
----------- | ---------------- | ----------------------- | ---
`1`         | `6.6`            | `v1.31`                 | N/A

(ref-csi-versioning-exceptions)=
### Special versions

- All versions before the first major release (`<1.0.0`) make no guarantees. Behavior may change even in patch releases.
- Versions with non-semantic tags (e.g. `latest-edge`) or with pre-release identifiers (e.g. `v1.2.3-edge`) are considered unstable and should be used only for testing.
- Version `0.0.0` is reserved for unstable builds where semantic versioning is required (e.g. `v0.0.0-latest-edge` is latest Helm chart release).

## Related topics

{{csi_exp}}

{{csi_how}}
