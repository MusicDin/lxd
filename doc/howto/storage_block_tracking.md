(howto-storage-block-tracking)=
# How to track changed blocks on virtual machine volumes

Changed block tracking records which blocks of a block volume the guest writes to after a snapshot, so that a backup tool copies only those blocks instead of the whole volume.
LXD implements it with QEMU dirty bitmaps that are created with an instance snapshot and named after it.
Each later snapshot with a bitmap gets a copy of every bitmap the volume has, and LXD serves the snapshot to an NBD client through the LXD API together with those copies.

A bitmap is kept across a stop, a reboot and a forced stop of the virtual machine.
It is deleted when its snapshot is deleted or renamed, when the QEMU process exits without writing it, for example on a crash or a host power loss, and when a volume is written by something other than the running virtual machine, for example a restore or a read-write NBD export.
After that the next backup must be a full one.

From the first start after the feature preview is enabled, every block disk of a virtual machine is opened through a small qcow2 image on its config volume, which stores the bitmaps of the volume.
A virtual machine that was running when the preview was enabled gets its images at its next start, and a snapshot with a bitmap is rejected until then.

## Requirements

- The `changed_block_tracking` feature preview enabled on both the server and the client (see {ref}`howto-snap-configure-feature-previews`). While it is disabled the server does not register the endpoints or advertise the extension, a snapshot with a bitmap is rejected, and the `lxc bitmap` and `lxc nbd` commands are hidden.
- The `storage_volume_block_tracking` API extension (see {ref}`extension-storage-volume-block-tracking`).
- A running virtual machine to create a bitmap. The bitmap is created on the root volume and, with `--disk-volumes all-exclusive`, on every attached `custom` volume of {ref}`content type <storage-content-types>` `block`.
- A volume with `security.shared` enabled does not get a bitmap, because several virtual machines can write to it at once and a bitmap records the writes of one virtual machine only.
- To write a volume over NBD, the virtual machine whose root volume it is or that it is attached to must be {ref}`stopped <instances-manage-stop>`.
- The `can_manage_snapshots` entitlement to take a snapshot with a bitmap, `can_view` to list bitmaps, and `can_connect_nbd` on the instance or the storage volume to read a snapshot or write a volume over NBD (see {ref}`permissions-reference`).
- An NBD client on the machine that runs the LXD client, for example `nbdinfo` and `nbdcopy` from `libnbd`, or `qemu-img`.

(storage-block-tracking-bitmaps)=
## Take a snapshot with a bitmap

A bitmap is created together with an instance snapshot and named after it.
It records the writes to the volume made after that snapshot, until the bitmap is deleted.
Taking the snapshot also copies every bitmap the volume already has into the snapshot. Such a copy records exactly the blocks written between the creation of the bitmap and the snapshot.

`````{tabs}
````{group-tab} CLI
Use the following command to snapshot a virtual machine and create a bitmap on its root volume:

    lxc snapshot <instance_name> <snapshot_name> --bitmap

Add `--disk-volumes all-exclusive` to snapshot the attached block volumes as well and create the bitmap on each of them.
````
````{group-tab} API
Set the `bitmap` field of the snapshot request:

    lxc query --request POST /1.0/instances/<instance_name>/snapshots --data '{"name": "<snapshot_name>", "bitmap": true}'

Set `disk_volumes_mode` to `all-exclusive` to cover the attached block volumes.

See [`POST /1.0/instances/{name}/snapshots`](swagger:/instances/instance_snapshots_post) for more information.
````
`````

The request is rejected when the instance is not a running virtual machine or when a snapshot of that name exists.

Every bitmap has a UUID next to its name, the UUID of the snapshot it was created with, which is the `volatile.uuid` of the root volume snapshot.
A snapshot of the same name that is created later, after the earlier snapshot was renamed or deleted, has another UUID.
A backup tool records the UUID with the name and passes it to the export of the next snapshot, so that it never reads changes recorded by a bitmap other than the one it stored.

## Manage bitmaps

`````{tabs}
````{group-tab} CLI
Use the following commands to list and show the bitmaps of a virtual machine:

    lxc bitmap list <instance_name>
    lxc bitmap show <instance_name> <bitmap_name>

The list prints one row per bitmap and volume, with the name and the UUID of the bitmap, the disk device, the pool, the type and the name of the volume, the granularity in bytes (the size of the block that one bit covers) and whether the bitmap is recording writes.

Use the following commands to list and show the copies that an instance snapshot keeps:

    lxc bitmap list <instance_name>/<snapshot_name>
    lxc bitmap show <instance_name>/<snapshot_name> <bitmap_name>

The copies of a snapshot are not recording, and a snapshot is never modified.
Deleting a snapshot removes the bitmap of its name from every volume of the virtual machine, and renaming a snapshot removes the bitmaps of its old and new names.
The snapshots keep their copies.
````
````{group-tab} API
Send the following requests to list and show the bitmaps of a virtual machine:

    lxc query --request GET /1.0/instances/<instance_name>/bitmaps?recursion=1
    lxc query --request GET /1.0/instances/<instance_name>/bitmaps/<bitmap_name>

Send the following requests to list and show the copies that an instance snapshot keeps:

    lxc query --request GET /1.0/instances/<instance_name>/snapshots/<snapshot_name>/bitmaps?recursion=1
    lxc query --request GET /1.0/instances/<instance_name>/snapshots/<snapshot_name>/bitmaps/<bitmap_name>

Each bitmap lists the volumes it exists on, with the pool, the type, the name and the UUID of the volume, the disk device it was attached through when the snapshot was taken, the granularity in bytes and whether the bitmap is recording writes.

See [`GET /1.0/instances/{name}/bitmaps`](swagger:/instances/instance_bitmaps_get) for more information.
````
`````

(storage-block-tracking-read)=
## Read a snapshot

An instance snapshot that was created with a bitmap is served read-only over NBD together with its copies of the bitmaps, which the export publishes as `qemu:dirty-bitmap:<bitmap_name>` metadata contexts next to `base:allocation`.
The export can be limited to the copy created with one snapshot, given by its UUID.
No copy is published when no snapshot has that UUID, for example because the snapshot was deleted and created again under the same name, and the backup tool then takes a full backup.
The virtual machine keeps running and is not affected by the export.

`````{tabs}
````{group-tab} CLI
Use the following command to serve an instance snapshot to a local NBD client:

    lxc nbd <instance_name>/<snapshot_name>

The command is not an NBD client.
It opens a local listener, prints the listening address, waits for one NBD client to connect, forwards that connection to LXD, and exits when the client disconnects:

    $ lxc nbd my-vm/snap1
    NBD listening on 127.0.0.1:41337

Add the `--address` flag to choose where to listen instead of a random port on the loopback interface.
Each run serves one client, and every client opens its own session, so several runs can serve the same snapshot at once.

Each volume snapshot is a separate NBD export named after its disk device, so the client selects a volume by adding the device name to the URL.
In a second terminal, point an NBD client at the printed address.
To list the blocks that a bitmap recorded up to the snapshot, use the following command:

    nbdinfo --map=qemu:dirty-bitmap:<bitmap_name> nbd://127.0.0.1:41337/root

To copy the whole root volume snapshot to a file, use the following command:

    nbdcopy --connections=1 nbd://127.0.0.1:41337/root <file_path>

Add the `--devices` flag with a comma separated list of disk device names to serve a subset of the volumes.
Add the `--previous-snapshot-uuid` flag with the UUID of the previous snapshot to serve only the copy of the bitmap created with it.
````
````{group-tab} API
Send a GET request with the `Upgrade: nbd` header to the NBD endpoint of the instance snapshot:

    GET /1.0/instances/<instance_name>/snapshots/<snapshot_name>/nbd

LXD answers with `101 Switching Protocols`, after which the client sends NBD commands over the connection.
Each volume snapshot is a separate NBD export named after its disk device, which the client selects during the NBD handshake.
The `device` query parameter, repeated once per disk device, selects a subset of the volumes.
The `previous_snapshot_uuid` query parameter limits the published copies to the one created with the snapshot of that UUID.

See [`GET /1.0/instances/{name}/snapshots/{snapshotName}/nbd`](swagger:/instances/instance_snapshot_nbd_get) for more information.
````
`````

To take incremental backups, take every snapshot with a bitmap.
The first snapshot is copied in full, as it has no copy of an earlier bitmap.
For every later snapshot, read the map of the bitmap created with the previous snapshot from the new snapshot's export, with the UUID of the previous snapshot, copy the blocks it marks, and delete the previous snapshot once the backup is stored.
Deleting the previous snapshot removes its bitmap from the virtual machine, and the new snapshot keeps its own copy of the bitmap.

(storage-block-tracking-restore)=
## Write a volume while the virtual machine is stopped

To restore a backup, write it into the volume through a read-write NBD export.
The virtual machine whose root volume it is, or that a `custom` volume is attached to, must be stopped.
To restore into a new virtual machine, first create it without an image:

    lxc init <instance_name> --empty --vm

`````{tabs}
````{group-tab} CLI
Use the following command to serve a volume read-write to a local NBD client:

    lxc storage volume nbd <pool_name> [<volume_type>/]<volume_name> --writable

The default volume type is `custom`.
The command prints the listening address (for example, `NBD listening on 127.0.0.1:41337`) and serves the volume under the default export.
Then write the backup into the export from a second terminal, with either of the following commands:

    qemu-img convert -n -f raw -O raw <file_path> nbd://127.0.0.1:41337
    nbdcopy <file_path> nbd://127.0.0.1:41337

The command requires `--writable`, as a confirmation that the volume is overwritten.
Start the virtual machine once the client has disconnected.
````
````{group-tab} API
Send a POST request with the `Upgrade: nbd` header to the NBD endpoint of the volume:

    POST /1.0/storage-pools/<pool_name>/volumes/<volume_type>/<volume_name>/nbd

LXD answers with `101 Switching Protocols`, after which the client sends NBD commands over the connection, including writes.

See [`POST /1.0/storage-pools/{poolName}/volumes/{type}/{volumeName}/nbd`](swagger:/storage/storage_pool_volumes_type_nbd_post) for more information.
````
`````

The bitmaps of the volume do not record the writes of the export, so they are deleted before the export starts.
The copies kept by the snapshots are not affected.

(storage-block-tracking-operations)=
## List and cancel NBD sessions

Every NBD session, whether a snapshot export or a volume export, is represented by an operation on the cluster member that serves it.
The operation runs for as long as the client stays connected, and cancelling it closes the connection.

Use the following commands to list the open sessions and to end one:

    lxc operation list
    lxc operation delete <operation_id>

When the `101 Switching Protocols` response comes from the member that serves the session, its `Location` header contains the URL of the operation.

## Limitations

- A crash or a host power loss deletes the bitmaps of the virtual machine, as does a live migration or a move to another cluster member.
  The copies kept by the snapshots are not affected, and the next backup must be a full one.
- A restore from a snapshot, a copy, a refresh, an import and a read-write NBD export write a volume while the virtual machine is stopped and delete its bitmaps.
  A restore of an instance snapshot deletes the bitmaps of every volume of the virtual machine.
- Deleting an instance snapshot removes the bitmap of its name from every volume, and renaming one removes the bitmaps of its old and new names, as the bitmaps are named after the snapshots.
  Renaming a custom volume snapshot does not affect the bitmaps.
- Deleting a custom volume snapshot removes the bitmap created with it from that volume, and leaves the instance snapshot that recorded it without the volume, which its export then skips.
- Resizing a volume deletes its bitmaps, as does detaching a `custom` volume from the virtual machine and enabling `security.shared` on a volume.
- The image a disk is opened through has 2 MiB clusters, and a discard by the guest that is smaller than a cluster is dropped.
  On a cluster, enabling `security.shared` on a custom volume attached to a virtual machine on another member must target that member.
- The virtual machine cannot be started, stopped or restarted, and none of its disks detached, while a snapshot with a bitmap is in progress or while one of its volumes is exported over NBD.
- An instance snapshot cannot be deleted or renamed, and neither can a custom volume snapshot it recorded, while the snapshot is exported over NBD.
- The copies of the bitmaps are stored on the config volume snapshot of the virtual machine, so the cluster member the instance is located on serves the snapshot.
