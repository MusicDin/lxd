test_storage_volume_initial_config() {

  lxd_backend=$(storage_backend "$LXD_DIR")
  if [ "$lxd_backend" != "zfs" ] && [ "$lxd_backend" != "lvm" ] && [ "$lxd_backend" != "ceph" ]; then
    return
  fi

  ensure_import_testimage

  image="testimage"
  profile="profile-initial-values"
  pool=$(lxc profile device get default root pool)

  if [ "$lxd_backend" = "zfs" ]; then
    lxc storage set "${pool}" volume.zfs.block_mode=true
  fi

  lxc profile create "${profile}"
  lxc profile device add "${profile}" root disk path=/ pool="${pool}"

  # Test default configuration (without initial configuration).
  lxc init "${image}" c --profile "${profile}"
  [ "$(lxc storage volume get "${pool}" container/c block.filesystem)" = "ext4" ]
  lxc rm c

  lxc init c --empty --profile "${profile}"
  [ "$(lxc storage volume get "${pool}" container/c block.filesystem)" = "ext4" ]
  lxc rm c

  # Test profile initial configuration.
  lxc profile device set "${profile}" root initial.block.filesystem=xfs

  lxc init "${image}" c --profile "${profile}"
  [ "$(lxc storage volume get "${pool}" container/c block.filesystem)" = "xfs" ]
  lxc rm c

  lxc init c --empty --profile "${profile}"
  [ "$(lxc storage volume get "${pool}" container/c block.filesystem)" = "xfs" ]
  lxc rm c

  # Test instance initial configuration.
  lxc init "${image}" c -s "${pool}" --no-profiles --device root,initial.block.filesystem=xfs
  [ "$(lxc storage volume get "${pool}" container/c block.filesystem)" = "xfs" ]
  lxc rm c

  lxc init c --empty -s "${pool}" --no-profiles --device root,initial.block.filesystem=xfs
  [ "$(lxc storage volume get "${pool}" container/c block.filesystem)" = "xfs" ]
  lxc rm c

  # Verify instance initial.* configuration modification.
  lxc init c --empty --no-profiles -s "${pool}" -d root,initial.block.filesystem=xfs
  ! lxc config device set c root initial.block.mount_options=noatime || false  # NOK: Add new configuration.
  ! lxc config device set c root initial.block.filesystem=ext4 || false        # NOK: Modify existing configuration.
  lxc config device set c root initial.block.filesystem=xfs                    # OK:  No change.
  lxc config device unset c root initial.block.filesystem                      # OK:  Remove existing configuration.
  lxc delete c --force

  lxc profile rm "${profile}"

  # Extra tests for ZFS storage driver.
  if [ "$lxd_backend" = "zfs" ]; then
    pool="storage-initial-values"

    # Create storage pool and enable block_mode for new volumes.
    lxc storage create "${pool}" zfs

    # Create profile and set previously created storage pool as root disk device.
    lxc profile create "${profile}"
    lxc profile device add "${profile}" root disk path=/ pool="${pool}"


    # > Verify zfs.block_mode without initial configuration.

    # Verify "zfs.block_mode=true" is applied from pool configuration.
    lxc storage set "${pool}" volume.zfs.block_mode=true

    lxc init c --empty --profile "${profile}"
    [ "$(lxc storage volume get "${pool}" container/c zfs.block_mode)" = "true" ]
    lxc delete c --force

    # Verify "zfs.block_mode=false" is applied from pool configuration.
    lxc storage set "${pool}" volume.zfs.block_mode=false

    lxc init c --empty --profile "${profile}"
    [ "$(lxc storage volume get "${pool}" container/c zfs.block_mode)" = "false" ]
    lxc delete c --force


    # > Overwrite zfs.block_mode with initial configuration in profile.

    # Verify instance "initial.zfs.block_mode=true" configuration is applied.
    lxc storage set "${pool}" volume.zfs.block_mode=false
    lxc profile device set "${profile}" root initial.zfs.block_mode=true

    lxc init c --empty --profile "${profile}"
    [ "$(lxc storage volume get "${pool}" container/c zfs.block_mode)" = "true" ]
    lxc delete c --force

    # Verify profile "initial.zfs.block_mode=false" configuration is applied.
    lxc storage set "${pool}" volume.zfs.block_mode=true
    lxc profile device set "${profile}" root initial.zfs.block_mode=false

    lxc init c --empty --profile "${profile}"
    [ "$(lxc storage volume get "${pool}" container/c zfs.block_mode)" = "false" ]
    lxc delete c --force


    # > Verify instance overwrite of initial.* configuration.

    # Verify instance "initial.zfs.block_mode=true" configuration is applied.
    lxc storage set "${pool}" volume.zfs.block_mode=false
    lxc profile device set "${profile}" root initial.zfs.block_mode=false

    lxc init c --empty --profile "${profile}" --device root,initial.zfs.block_mode=true
    [ "$(lxc storage volume get "${pool}" container/c zfs.block_mode)" = "true" ]
    lxc delete c --force

    # Verify instance "initial.zfs.block_mode=false" configuration is applied.
    lxc storage set "${pool}" volume.zfs.block_mode=true
    lxc profile device set "${profile}" root initial.zfs.block_mode=true

    lxc init c --empty --profile "${profile}" --device root,initial.zfs.block_mode=false
    [ "$(lxc storage volume get "${pool}" container/c zfs.block_mode)" = "false" ]
    lxc delete c --force


    # > Verify initial.zfs.blocksize configuration.

    # Custom blocksize.
    lxc init "${image}" c --no-profiles --storage "${pool}" --device root,initial.zfs.blocksize=64KiB
    [ "$(lxc storage volume get "${pool}" container/c zfs.blocksize)" = "64KiB" ]
    [ "$(zfs get volblocksize ${pool}/containers/c -H -o value)" = "64K" ]
    lxc delete c --force

    # Custom blocksize that exceeds maximum allowed blocksize.
    lxc init "${image}" c --no-profiles --storage "${pool}" --device root,initial.zfs.blocksize=512KiB
    [ "$(lxc storage volume get "${pool}" container/c zfs.blocksize)" = "512KiB" ]
    [ "$(zfs get volblocksize ${pool}/containers/c -H -o value)" = "128K" ]
    lxc delete c --force

    # Cleanup
    lxc profile delete "${profile}"
    lxc storage delete "${pool}"
  fi
}
