#!/bin/bash

# test_clustering_live_migration spawns a 2-node LXD cluster, creates a virtual machine on top of it,
# creates and attaches a block volume to the virtual machine, writes some arbitrary data to the volume,
# and runs live migration. Success is determined by the data being intact after live migration.
test_clustering_live_migration_intra_cluster() {
  poolDriver="$(storage_backend "${LXD_INITIAL_DIR}")"
  if [ "${poolDriver}" = "lvm" ]; then
    export TEST_UNMET_REQUIREMENT="Storage driver ${poolDriver} is currently unsupported"
    return 0
  fi

  # For remote storage drivers, we perform the live migration with custom storage pool attached as well.
  isRemoteDriver=false
  if [ "${poolDriver}" == "ceph" ]; then
    isRemoteDriver=true
  fi

  # Spawn the first node and bootstrap the cluster.
  if [ "${poolDriver}" == "ceph" ]; then
    # Set test live migration env var to prevent LXD erroring out during unmount of the
    # source instance volume during live migration on the same host. During unmount the
    # volume is already mounted to the destination instance and will error with "device
    # or resource busy" error.
    LXD_TEST_LIVE_MIGRATION_ON_THE_SAME_HOST=true spawn_lxd_and_bootstrap_cluster "${poolDriver}"
  else
    spawn_lxd_and_bootstrap_cluster "${poolDriver}"
  fi

  local cert
  cert="$(cert_to_yaml "${LXD_ONE_DIR}/cluster.crt")"

  # Spawn a second node.
  if [ "${poolDriver}" == "ceph" ]; then
    LXD_TEST_LIVE_MIGRATION_ON_THE_SAME_HOST=true spawn_lxd_and_join_cluster "${cert}" 2 1 "${LXD_ONE_DIR}" "${poolDriver}"
  else
    spawn_lxd_and_join_cluster "${cert}" 2 1 "${LXD_ONE_DIR}" "${poolDriver}"
  fi

  # Set up a TLS identity with admin permissions.
  LXD_DIR="${LXD_ONE_DIR}" lxc auth group create live-migration
  LXD_DIR="${LXD_ONE_DIR}" lxc auth group permission add live-migration server admin

  token="$(LXD_DIR="${LXD_ONE_DIR}" lxc auth identity create tls/live-migration --group=live-migration --quiet)"
  LXD_DIR="${LXD_ONE_DIR}" lxc remote add cls 100.64.1.101:8443 --token="${token}"

  LXD_DIR="${LXD_ONE_DIR}" ensure_import_ubuntu_vm_image

  # Storage pool created when spawning LXD cluster is "data".
  poolName="data"
  LXD_DIR="${LXD_ONE_DIR}" lxc storage set "${poolName}" volume.size="${SMALLEST_VM_ROOT_DISK}"

  # Initialize the VM.
  LXD_DIR="${LXD_ONE_DIR}" lxc init ubuntu-vm vm \
    --vm \
    --config limits.cpu=2 \
    --config limits.memory=768MiB \
    --config migration.stateful=true \
    --device root,size="${SMALLEST_VM_ROOT_DISK}" \
    --target node1

  # For remote storage drivers, test live migration with custom volume as well.
  if [ "${isRemoteDriver}" = true ]; then
    # Attach the block volume to the VM.
    LXD_DIR="${LXD_ONE_DIR}" lxc storage volume create "${poolName}" vmdata --type=block size=1MiB
    LXD_DIR="${LXD_ONE_DIR}" lxc config device add vm vmdata disk pool="${poolName}" source=vmdata
  fi

  # Start the VM.
  LXD_DIR="${LXD_ONE_DIR}" lxc start vm
  LXD_DIR="${LXD_ONE_DIR}" waitInstanceReady vm

  # Inside the VM, format and mount the volume, then write some data to it.
  if [ "${isRemoteDriver}" = true ]; then
    LXD_DIR="${LXD_ONE_DIR}" lxc exec vm -- mkfs -t ext4 /dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_vmdata
    LXD_DIR="${LXD_ONE_DIR}" lxc exec vm -- mkdir /mnt/vol1
    LXD_DIR="${LXD_ONE_DIR}" lxc exec vm -- mount -t ext4 /dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_vmdata /mnt/vol1
    LXD_DIR="${LXD_ONE_DIR}" lxc exec vm -- cp /etc/hostname /mnt/vol1/bar
  fi

  # Perform live migration of the VM from node1 to node2.
  echo "Live migrating instance 'vm' ..."
  LXD_DIR="${LXD_ONE_DIR}" lxc move vm --target node2
  LXD_DIR="${LXD_ONE_DIR}" waitInstanceReady vm

  # After live migration, the volume should be functional and mounted.
  # Check that the file we created is still there with the same contents.
  echo "Verifying data integrity after live migration"
  if [ "${isRemoteDriver}" = true ]; then
    [ "$(LXD_DIR=${LXD_ONE_DIR} lxc exec vm -- cat /mnt/vol1/bar)" = "vm" ]
  fi

  # Cleanup
  echo "Cleaning up ..."
  LXD_DIR="${LXD_ONE_DIR}" lxc image delete "$(LXD_DIR="${LXD_ONE_DIR}" lxc config get vm volatile.base_image)"
  LXD_DIR="${LXD_ONE_DIR}" lxc delete --force vm

  if [ "${isRemoteDriver}" = true ]; then
    LXD_DIR="${LXD_ONE_DIR}" lxc storage volume delete "${poolName}" vmdata
  fi

  # Ensure cleanup of the cluster's data pool to not leave any traces behind when we are using a different driver besides dir.
  printf 'config: {}\ndevices: {}' | LXD_DIR="${LXD_ONE_DIR}" lxc profile edit default
  LXD_DIR="${LXD_ONE_DIR}" lxc storage delete "${poolName}"

  # DEBUG: Show storage pool info
  echo ">>> DEBUG AFTER CLEANUP <<<"
  LXD_DIR="${LXD_ONE_DIR}" lxc storage ls || true
  LXD_DIR="${LXD_ONE_DIR}" lxc image ls || true
  # DEBUG: END

  lxc remote remove cls

  LXD_DIR="${LXD_ONE_DIR}" lxd shutdown
  LXD_DIR="${LXD_TWO_DIR}" lxd shutdown

  rm -f "${LXD_ONE_DIR}/unix.socket"
  rm -f "${LXD_TWO_DIR}/unix.socket"

  teardown_clustering_netns
  teardown_clustering_bridge

  kill_lxd "${LXD_ONE_DIR}"
  kill_lxd "${LXD_TWO_DIR}"
}

# test_clustering_live_migration_diff_servers spawns 2 LXD servers, creates a virtual machine on the first one,
# and live migrates it to the second one. For remote storage drivers, an additional custom volume is attached
# to the virtual machine.
test_clustering_live_migration() {
  poolDriver="$(storage_backend "${LXD_INITIAL_DIR}")"

  # For remote storage drivers, we perform the live migration with custom storage pool attached as well.
  isRemoteDriver=false
  if [ "${poolDriver}" == "ceph" ]; then
    isRemoteDriver=true

    # Set test live migration env var to prevent LXD erroring out during unmount of the
    # source instance volume during live migration on the same host. During unmount the
    # volume is already mounted to the destination instance and will error with "device
    # or resource busy" error.
    export LXD_TEST_LIVE_MIGRATION_ON_THE_SAME_HOST=true
  fi

  # Spawn a two LXD servers.
  LXD_ONE_DIR="$(mktemp -d -p "${TEST_DIR}" XXX)"
  LXD_TWO_DIR="$(mktemp -d -p "${TEST_DIR}" XXX)"
  spawn_lxd "${LXD_ONE_DIR}" true
  spawn_lxd "${LXD_TWO_DIR}" true

  # Set up a TLS identity with admin permissions.
  LXD_DIR="${LXD_TWO_DIR}" lxc auth group create live-migration
  LXD_DIR="${LXD_TWO_DIR}" lxc auth group permission add live-migration server admin

  # Add second LXD as remote to the first LXD.
  token="$(LXD_DIR="${LXD_TWO_DIR}" lxc auth identity create tls/live-migration --group=live-migration --quiet)"
  address="$(LXD_DIR="${LXD_TWO_DIR}" lxc config get core.https_address)"
  LXD_DIR="${LXD_TWO_DIR}" lxc remote add dst "${address}" --token="${token}"

  LXD_DIR="${LXD_ONE_DIR}" ensure_import_ubuntu_vm_image

  # Get names of the created storage pools on both LXD servers.
  srcPoolName="$(LXD_DIR="${LXD_ONE_DIR}" lxc profile device get default root pool)"
  dstPoolName="$(LXD_DIR="${LXD_TWO_DIR}" lxc profile device get default root pool)"

  LXD_DIR="${LXD_ONE_DIR}" lxc storage set "${srcPoolName}" volume.size="${SMALLEST_VM_ROOT_DISK}"
  LXD_DIR="${LXD_TWO_DIR}" lxc storage set "${dstPoolName}" volume.size="${SMALLEST_VM_ROOT_DISK}"

  # Initialize the VM.
  LXD_DIR="${LXD_ONE_DIR}" lxc init ubuntu-vm vm \
    --vm \
    --config limits.cpu=2 \
    --config limits.memory=768MiB \
    --config migration.stateful=true \
    --storage "${srcPoolName}" \
    --device root,size="${SMALLEST_VM_ROOT_DISK}"

  liveMigrationOpts=()

  # For remote storage drivers, test live migration with custom volume as well.
  if [ "${isRemoteDriver}" = true ]; then
    # Attach the block volume to the VM.
    LXD_DIR="${LXD_ONE_DIR}" lxc storage volume create "${srcPoolName}" vmdata --type=block size=1MiB
    LXD_DIR="${LXD_ONE_DIR}" lxc config device add vm vmdata disk pool="${srcPoolName}" source=vmdata

    # Specify the destination pool for the custom volume.
    liveMigrationOpts+=(--device "vmdata,pool=${dstPoolName}")
  fi

  # Start the VM.
  LXD_DIR="${LXD_ONE_DIR}" lxc start vm
  LXD_DIR="${LXD_ONE_DIR}" waitInstanceReady vm

  # Inside the VM, format and mount the volume, then write some data to it.
  if [ "${isRemoteDriver}" = true ]; then
    LXD_DIR="${LXD_ONE_DIR}" lxc exec vm -- mkfs -t ext4 /dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_vmdata
    LXD_DIR="${LXD_ONE_DIR}" lxc exec vm -- mkdir /mnt/vol1
    LXD_DIR="${LXD_ONE_DIR}" lxc exec vm -- mount -t ext4 /dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_vmdata /mnt/vol1
    LXD_DIR="${LXD_ONE_DIR}" lxc exec vm -- cp /etc/hostname /mnt/vol1/bar
  fi

  echo ">>> DEBUG BEFORE MOVE <<<"
  echo "LXD ONE STORAGE POOL: ${srcPoolName}"
  echo "LXD TWO STORAGE POOL: ${dstPoolName}"

  LXD_DIR="${LXD_ONE_DIR}" lxc config show vm || true
  LXD_DIR="${LXD_ONE_DIR}" lxc storage ls || true
  LXD_DIR="${LXD_TWO_DIR}" lxc storage ls || true
  echo ">>> DEBUG: END <<<"

  # Perform live migration of the VM from one server to another.
  echo "Live migrating instance 'vm' ..."
  LXD_DIR="${LXD_ONE_DIR}" lxc move vm dst:vm --storage "${dstPoolName}" "${liveMigrationOpts[@]}"
  LXD_DIR="${LXD_TWO_DIR}" waitInstanceReady vm

  # After live migration, the volume should be functional and mounted.
  # Check that the file we created is still there with the same contents.
  if [ "${isRemoteDriver}" = true ]; then
    echo "Verifying data integrity after live migration"
    [ "$(LXD_DIR="${LXD_TWO_DIR}" lxc exec vm -- cat /mnt/vol1/bar)" = "vm" ]
  fi

  # Cleanup
  echo "Cleaning up ..."
  LXD_DIR="${LXD_ONE_DIR}" lxc image delete "$(LXD_DIR="${LXD_TWO_DIR}" lxc config get vm volatile.base_image)"
  LXD_DIR="${LXD_TWO_DIR}" lxc delete --force vm

  if [ "${isRemoteDriver}" = true ]; then
    LXD_DIR="${LXD_TWO_DIR}" lxc storage volume delete "${dstPoolName}" vmdata
  fi

  # Ensure cleanup of the storage pools to not leave any traces behind.
  unset LXD_TEST_LIVE_MIGRATION_ON_THE_SAME_HOST
  printf 'config: {}\ndevices: {}' | LXD_DIR="${LXD_ONE_DIR}" lxc profile edit default
  printf 'config: {}\ndevices: {}' | LXD_DIR="${LXD_TWO_DIR}" lxc profile edit default
  LXD_DIR="${LXD_ONE_DIR}" lxc storage delete "${srcPoolName}"
  LXD_DIR="${LXD_TWO_DIR}" lxc storage delete "${dstPoolName}"

  # DEBUG: Show storage pool info
  echo ">>> DEBUG AFTER CLEANUP <<<"
  LXD_DIR="${LXD_ONE_DIR}" lxc storage ls || true
  LXD_DIR="${LXD_TWO_DIR}" lxc storage ls || true
  LXD_DIR="${LXD_ONE_DIR}" lxc image ls || true
  LXD_DIR="${LXD_TWO_DIR}" lxc image ls || true
  echo ">>> DEBUG: END <<<"

  lxc remote remove dst

  LXD_DIR="${LXD_ONE_DIR}" lxd shutdown
  LXD_DIR="${LXD_TWO_DIR}" lxd shutdown

  kill_lxd "${LXD_ONE_DIR}"
  kill_lxd "${LXD_TWO_DIR}"
}
