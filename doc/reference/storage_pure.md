(storage-pure)=
# PureStorage - `pure`

[PureStorage](https://www.purestorage.com/) is a software-defined storage solution. It offers the consumption of redundant block storage across the network.

LXD offers access to PureStorage storage clusters using iSCSI.
In addition, PureStorage offers copy-on-write snapshots, thin provisioning and other features.

To use PureStorage with iSCSI, make sure you have the required kernel modules installed on your host system.
LXD takes care of connecting to the respective subsystem.

## Terminology

Each storage pool created in LXD using PureStorage driver represents a PureStorage *pod*, which is an abstraction that groups multiple volumes under a specific name.
Another benefit of using PureStorage pods, is that they can be linked with multiple PureStorage arrays to provide additional redundancy.
LXD created volumes within a pod that is identified by the storage pool name.
When first volume is mapped to a specific LXD host, a corresponding PureStorage host is created with the name of the LXD host.
The PureStorage host is then connected with the required volumes, to allow attaching and accesing volumes from the LXD host.
Created PureStorage hosts are removed once there is no volume connected to it.

## `pure` driver in LXD

The `pure` driver in LXD uses PureStorage volumes for custom storage volumes, instances, and snapshots.
All created volumes are thin-provisioned block volumes, that LXD formats with a desired filesystem if required (for containers and custom filesystem volumes).

LXD expects the PureStorage to be preconfigured with a specific service (e.g. iSCSI) on network interfaces whose address is provided during storage pool configuration.
Furthermore, LXD assumes that it has full control over the PureStorage pods it manages.
Therefore, you should never maintain any volumes in PureStorage pods that are not owned by LXD because LXD might delete them.

This driver behaves differently than some of the other drivers in that it provides remote storage.
As a result and depending on the internal network, storage access might be a bit slower than for local storage.
On the other hand, using remote storage has big advantages in a cluster setup, because all cluster members have access to the same storage pools with the exact same contents, without the need to synchronize them.

When creating a new storage pool using the `pure` driver in `iscsi` mode, LXD tries to discover one of the array's IQNs on the `pure.iscsi.address`.
Upon successful discovery, LXD attaches all volumes that are connected to the PureStorage host that is associated with a specific LXD server.
PureStorage hosts and volume connections are completely managed by LXD.

Volume snapshots are associated with a parent volume, and they cannot be directly attached to the host.
Therefore, when a snapshot is being exported, LXD creates a temporary volume behind the scene, which is attached to the LXD host.
Similarly, when a volume with at least one snapshot is being copied, LXD sequentially copies snapshots into destination volume, from which a new snapshot is created.
Finally, once all snapshots are copied, source volume is copied into destination volume.

(storage-pure-volume-names)=
### Volume names

Due to a [limitation](storage-pure-limitations) in PureStorage, volume names cannot exceed 63 characters.
Therefore the driver is using the volume's {config:option}`storage-pure-volume-conf:volatile.uuid` to generate a shorter volume name.
For example, a UUID `5a2504b0-6a6c-4849-8ee7-ddb0b674fd14` is first trimmed of hyphens (`-`) resulting in string `5a2504b06a6c48498ee7ddb0b674fd14`.

To be able to identify the volume types and snapshots, special identifiers are prepended and appended to the volume names:

Type            | Identifier   | Example
:--             | :---         | :----------
Container       | `c-`         | `c-5a2504b06a6c48498ee7ddb0b674fd14`
Virtual machine | `v-`         | `v-5a2504b06a6c48498ee7ddb0b674fd14-b` (block volume) and `v-5a2504b06a6c48498ee7ddb0b674fd14` (filesystem volume)
Image (ISO)     | `i-`         | `i-5a2504b06a6c48498ee7ddb0b674fd14-i`
Custom volume   | `u-`         | `u-5a2504b06a6c48498ee7ddb0b674fd14`
Snapshot        | `s`          | `sc-5a2504b06a6c48498ee7ddb0b674fd14` (container snapshot)

(storage-pure-limitations)=
### Limitations

The `pure` driver has the following limitations:

- PureStorage allows creating up to 400.000 snapshots.

- Minimum volume size is `1MiB` and has to be a multiply of `512B`.

- Snapshots cannot be mounted to the host, which requires creating a temporary volume when accessing snapshot's contents. This is entierly handeld by LXD.

Volume size constraints
: Minimum volume size is `1MiB` and has to be a multiply of `512B`.

Snapshots cannot be mounted
: Snapshots cannot be mounted directly to the host, which requires creating a temporary volume when accessing snapshot's contents.
  For internal operations, such as copying instances or exporting snapshots, LXD handels this automatically.

Sharing the PureStorage storage pool between installations
: Sharing the same PureStorage storage pool between multiple LXD installations is not supported.
  If different LXD installation tries to create a storage pool with the name that already exists, an error is be returned.

Recovering PureStorage storage pools
: Recovery of PureStorage storage pools using `lxd recover` is currently not supported.

## Configuration options

The following configuration options are available for storage pools that use the `pure` driver and for storage volumes in these pools.

(storage-pure-pool-config)=
### Storage pool configuration

% Include content from [../metadata.txt](../metadata.txt)
```{include} ../metadata.txt
    :start-after: <!-- config group storage-pure-pool-conf start -->
    :end-before: <!-- config group storage-pure-pool-conf end -->
```

{{volume_configuration}}

(storage-pure-vol-config)=
### Storage volume configuration

% Include content from [../metadata.txt](../metadata.txt)
```{include} ../metadata.txt
    :start-after: <!-- config group storage-pure-volume-conf start -->
    :end-before: <!-- config group storage-pure-volume-conf end -->
```
