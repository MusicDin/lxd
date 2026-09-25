# _nbd_serve runs an lxc NBD command in the background and sets NBD_PID, NBD_URI and NBD_STDERR once the
# command prints the address it listens on. The export contacts LXD only when a client connects, so a
# precondition error surfaces in NBD_STDERR after the first client. The lxc wrapper kills its command after
# 120s, which a full copy of the root disk can exceed, so the binary is called directly.
_nbd_serve() {
  local output address
  output="$(mktemp -p "${TEST_DIR}" nbd_output.XXX)"
  NBD_STDERR="$(mktemp -p "${TEST_DIR}" nbd_stderr.XXX)"

  "${_LXC}" "$@" > "${output}" 2> "${NBD_STDERR}" &
  NBD_PID=$!

  for _ in $(seq 60); do
    grep -qF "NBD listening on " "${output}" && break
    sleep 0.5
  done

  address="$(sed -n 's/^NBD listening on //p' "${output}")"
  rm "${output}"
  [ -n "${address}" ]
  NBD_URI="nbd://${address}"
}

# _nbd_hold keeps an NBD session of the export at NBD_URI open until _nbd_release runs. nc ignores its stdin so
# that it exits once the server closes the connection.
_nbd_hold() {
  local address
  address="${NBD_URI#nbd://}"
  nc -d "${address%:*}" "${address##*:}" &
  NBD_HOLDER_PID=$!
  sleep 1
}

# _nbd_release ends the session opened by _nbd_hold and waits for the export command to exit.
_nbd_release() {
  kill "${NBD_HOLDER_PID}" 2>/dev/null || true
  wait "${NBD_HOLDER_PID}" || true
  wait "${NBD_PID}" || true
}

# _nbd_contexts prints the metadata contexts of the named export of the snapshot NBD command given as the remaining
# arguments, as a JSON array, and waits for the command to exit.
_nbd_contexts() {
  local export_name="$1"
  shift
  _nbd_serve "$@"
  nbdinfo --list --json "${NBD_URI}" | jq --compact-output --arg name "${export_name}" '[.exports[] | select(."export-name" == $name)][0].contexts'
  wait "${NBD_PID}"
}

# _bitmaps prints the bitmaps of an instance or of an instance snapshot as "name,device,recording" lines, sorted.
_bitmaps() {
  lxc bitmap list "$@" --format csv -c ndr | sort
}

# _bitmap_uuid prints the UUID of the named bitmap of an instance or of an instance snapshot.
_bitmap_uuid() {
  lxc bitmap show "$1" "$2" | yq --exit-status '.uuid'
}

# _snapshot_uuid prints the instance snapshot UUID of a snapshot, the UUID of its root volume snapshot.
_snapshot_uuid() {
  lxc storage volume get "${pool}" "virtual-machine/$1" volatile.uuid
}

# _volume_snapshot_of prints the name of the snapshot of the custom volume cbt-blk that was taken with the given
# instance snapshot, which the instance snapshot records in volatile.attached_volumes.
_volume_snapshot_of() {
  local uuid snap
  uuid="$(lxc config show "v1/$1" | yq --exit-status '.config."volatile.attached_volumes"' | jq --raw-output '."cbt-blk"')"
  for snap in $(lxc storage volume snapshot list "${pool}" cbt-blk --format csv -c n); do
    if [ "$(lxc storage volume get "${pool}" "cbt-blk/${snap}" volatile.uuid)" = "${uuid}" ]; then
      echo "${snap}"
      return 0
    fi
  done

  return 1
}

# _wait_stopped waits for the instance to reach the STOPPED state.
_wait_stopped() {
  for _ in $(seq 60); do
    [ "$(lxc list -f csv -c s "$1")" = "STOPPED" ] && return 0
    sleep 1
  done

  return 1
}

# _write_overlay creates an overlay file for the volume of the given UUID on the live config volume of v1, with a
# 64 KiB pattern written at its start, as a crash during a snapshot with a bitmap leaves one. It prints the checksum
# of the pattern.
_write_overlay() {
  local overlay="${metadata_images}/$1.overlay.qcow2"
  qemu-img create -f qcow2 "${overlay}" "$2" > /dev/null
  qemu-io -f qcow2 -c "write -P 0xab 0 64k" "${overlay}" > /dev/null
  head -c 65536 /dev/zero | tr '\0' '\253' | sha256sum | cut -d' ' -f1
}

test_storage_block_tracking_vm() {
  if ! check_dependencies nbdinfo nbdcopy qemu-img qemu-io qemu-nbd qemu-storage-daemon; then
    export TEST_UNMET_REQUIREMENT="Missing nbdinfo, nbdcopy, qemu-img, qemu-io, qemu-nbd or qemu-storage-daemon"
    return
  fi

  local pool orig_volume_size root_dev root_size s1_uuid s1b_uuid s2_uuid s2b_uuid s3_uuid s1_copy s3_copy reconstructed extents offset length chunk blk_dev blk_size blk_checksum blk_copy operation_uuid pid metadata_images root_uuid blk_uuid pattern import_src snap_a snap_b
  pool="lxdtest-$(basename "${LXD_DIR}")"
  orig_volume_size="$(lxc storage get "${pool}" volume.size)"
  if [ -n "${orig_volume_size:-}" ]; then
    # Override the volume.size to accommodate a VM
    lxc storage set "${pool}" volume.size "${SMALLEST_VM_ROOT_DISK}"
  fi

  ensure_import_ubuntu_vm_image

  lxc init ubuntu-vm v1 --vm -c limits.memory=384MiB -d "${SMALL_VM_ROOT_DISK}"
  lxc storage volume create "${pool}" cbt-blk size=32MiB --type block
  lxc storage volume attach "${pool}" cbt-blk v1
  lxc start v1
  waitInstanceReady v1

  setup_instance_gocoverage v1

  metadata_images="${LXD_DIR}/virtual-machines/v1/metadata_images"
  root_uuid="$(lxc storage volume get "${pool}" virtual-machine/v1 volatile.uuid)"
  blk_uuid="$(lxc storage volume get "${pool}" cbt-blk volatile.uuid)"

  sub_test "Every block disk is opened through its volume metadata image"
  # The images exist from the first start on, before any bitmap is created, and no snapshot metadata image is left.
  [ -e "${metadata_images}/${root_uuid}.qcow2" ]
  [ -e "${metadata_images}/${blk_uuid}.qcow2" ]
  [ "$(find "${metadata_images}" -name '*.qcow2' | wc -l)" = "2" ]
  [ "$(_bitmaps v1)" = "" ]

  sub_test "A snapshot with a bitmap creates the bitmap on the root disk"
  lxc snapshot v1 s1 --bitmap
  [ "$(_bitmaps v1)" = "s1,root,YES" ]
  lxc bitmap show v1 s1 | yq --exit-status '.name == "s1" and (.volumes | length) == 1 and .volumes[0].device == "root" and .volumes[0].type == "virtual-machine" and .volumes[0].name == "v1" and .volumes[0].recording == true and .volumes[0].granularity == 65536'
  [ "$(lxc bitmap show v1 s1 | yq --exit-status '.volumes[0].uuid')" = "${root_uuid}" ]
  [ "$(lxc bitmap show v1 s1 | yq --exit-status '.volumes[0].pool')" = "${pool}" ]
  [ "$(! "${_LXC}" bitmap show v1 missing 2>&1 1>/dev/null)" = "Error: Bitmap not found" ]

  # The UUID of the bitmap is the instance snapshot UUID of the snapshot it was created with.
  s1_uuid="$(_bitmap_uuid v1 s1)"
  [ "${s1_uuid}" = "$(_snapshot_uuid v1/s1)" ]

  # The snapshot metadata image is removed from the config volume once the config volume snapshot includes it.
  [ "$(find "${metadata_images}" -name "${root_uuid}.*.qcow2" | wc -l)" = "0" ]

  # A snapshot of a name that exists is rejected when the request is received.
  [ "$(! "${_LXC}" snapshot v1 s1 --bitmap 2>&1 1>/dev/null)" = 'Error: Snapshot "s1" already exists' ]
  [ "$(! "${_LXC}" snapshot v1 s1 2>&1 1>/dev/null)" = 'Error: Snapshot "s1" already exists' ]

  # The first snapshot with a bitmap has an empty snapshot metadata image, so its export offers the allocation map only.
  [ "$(_bitmaps v1/s1)" = "" ]
  [ "$(_nbd_contexts root nbd v1/s1)" = '["base:allocation"]' ]

  sub_test "A snapshot without a bitmap keeps the bitmaps and has no export"
  lxc snapshot v1 p1
  [ "$(_bitmaps v1)" = "s1,root,YES" ]
  [ "$(_bitmaps v1/p1)" = "" ]
  _nbd_serve nbd v1/p1
  ! nbdinfo "${NBD_URI}" || false
  ! wait "${NBD_PID}" || false
  [[ "$(cat "${NBD_STDERR}")" == "Error: Snapshot was not created with a bitmap"* ]]

  # Dirty a few MiB of the root disk.
  lxc exec v1 -- sh -c 'dd if=/dev/urandom of=/root/cbt.bin bs=1M count=4 && sync'

  sub_test "The next snapshot gets a copy of the bitmap"
  lxc snapshot v1 s2 --bitmap
  s2_uuid="$(_bitmap_uuid v1 s2)"
  [ "${s2_uuid}" = "$(_snapshot_uuid v1/s2)" ]
  [ "${s2_uuid}" != "${s1_uuid}" ]
  [ "$(_bitmaps v1)" = "$(printf 's1,root,YES\ns2,root,YES')" ]
  [ "$(_bitmaps v1/s2)" = "s1,root,NO" ]
  [ "$(_bitmap_uuid v1/s2 s1)" = "${s1_uuid}" ]
  lxc bitmap show v1/s2 s1 | yq --exit-status '.volumes[0].device == "root" and .volumes[0].type == "virtual-machine" and .volumes[0].name == "v1" and .volumes[0].recording == false'
  [ "$(! "${_LXC}" bitmap show v1/s2 s2 2>&1 1>/dev/null)" = "Error: Bitmap not found" ]
  [ "$(! "${_LXC}" bitmap show v1/missing s1 2>&1 1>/dev/null)" = 'Error: Instance snapshot not found' ]

  sub_test "The snapshot export exposes the bitmaps of the snapshot"
  root_dev="/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_root"
  root_size="$(lxc exec v1 -- blockdev --getsize64 "${root_dev}")"

  # The export is read-only, has the size of the disk and exposes the bitmap of the snapshot, but not the bitmap
  # created with the snapshot.
  _nbd_serve nbd v1/s2
  nbdinfo --list --json "${NBD_URI}" | jq --exit-status --argjson size "${root_size}" '[.exports[] | select(."export-name" == "root")][0] | .is_read_only and (."export-size" == $size) and (.contexts == ["base:allocation", "qemu:dirty-bitmap:s1"])'
  wait "${NBD_PID}"

  # The previous snapshot UUID limits the exposed bitmaps to the ones created with that snapshot, and an unknown
  # UUID exposes none.
  [ "$(_nbd_contexts root nbd v1/s2 --previous-snapshot-uuid "${s1_uuid}")" = '["base:allocation","qemu:dirty-bitmap:s1"]' ]
  [ "$(_nbd_contexts root nbd v1/s2 --previous-snapshot-uuid "${s2_uuid}")" = '["base:allocation"]' ]
  [ "$(_nbd_contexts root nbd v1/s2 --previous-snapshot-uuid 00000000-0000-0000-0000-000000000000)" = '["base:allocation"]' ]

  # A device that is not part of the snapshot is not found.
  _nbd_serve nbd v1/s2 --devices missing
  ! nbdinfo "${NBD_URI}" || false
  ! wait "${NBD_PID}" || false
  [[ "$(cat "${NBD_STDERR}")" == 'Error: Snapshot has no volume snapshot with bitmaps for device "missing"'* ]]

  sub_test "An incremental backup reconstructs the second snapshot from the first"
  s1_copy="$(mktemp -p "${TEST_DIR}" s1_copy.XXX)"
  s3_copy="$(mktemp -p "${TEST_DIR}" s3_copy.XXX)"
  reconstructed="$(mktemp -p "${TEST_DIR}" reconstructed.XXX)"

  # Full copy of s1, then the blocks that the bitmap s1 marks in s2 on top of it.
  _nbd_serve nbd v1/s1
  nbdcopy --connections=1 "${NBD_URI}/root" "${s1_copy}"
  wait "${NBD_PID}"
  [ "$(stat -c %s "${s1_copy}")" = "${root_size}" ]

  cp "${s1_copy}" "${reconstructed}"
  _nbd_serve nbd v1/s2 --previous-snapshot-uuid "${s1_uuid}"
  extents="$(nbdinfo --json --map=qemu:dirty-bitmap:s1 "${NBD_URI}/root" | jq --compact-output '[.[] | select(.type == 1) | [.offset, .length]]')"
  [ "$(echo "${extents}" | jq 'length')" -gt 0 ]
  chunk="$(mktemp -p "${TEST_DIR}" chunk.XXX)"
  while read -r offset length; do
    nbdcopy --connections=1 --offset="${offset}" --length="${length}" "${NBD_URI}/root" "${chunk}"
    dd if="${chunk}" of="${reconstructed}" bs=64K seek="$((offset / 65536))" conv=notrunc status=none
  done < <(echo "${extents}" | jq --raw-output '.[] | "\(.[0]) \(.[1])"')
  rm -f "${chunk}"
  wait "${NBD_PID}"

  _nbd_serve nbd v1/s2
  nbdcopy --connections=1 "${NBD_URI}/root" "${s3_copy}"
  wait "${NBD_PID}"
  [ "$(sha256sum "${reconstructed}" | cut -d' ' -f1)" = "$(sha256sum "${s3_copy}" | cut -d' ' -f1)" ]
  rm -f "${reconstructed}"

  sub_test "Every client opens its own session"
  _nbd_serve nbd v1/s2
  _nbd_hold
  [ "$(_nbd_contexts root nbd v1/s2)" = '["base:allocation","qemu:dirty-bitmap:s1"]' ]
  _nbd_release

  sub_test "NBD export is listed as an operation and cancelling it ends the export"
  _nbd_serve nbd v1/s2
  _nbd_hold
  operation_uuid="$(lxc operation list --format json | jq --exit-status --raw-output '[.[] | select(.description == "Exporting instance snapshot over NBD")][0].id')"
  [ -n "${operation_uuid}" ]
  lxc operation delete "${operation_uuid}"
  wait "${NBD_PID}" || true
  kill "${NBD_HOLDER_PID}" 2>/dev/null || true
  wait "${NBD_HOLDER_PID}" || true

  sub_test "Deleting a snapshot removes the bitmap of its name"
  lxc delete v1/s1
  [ "$(_bitmaps v1)" = "s2,root,YES" ]

  # The copies of the snapshots stay, but the export exposes none for a snapshot that no longer exists.
  [ "$(_bitmaps v1/s2)" = "s1,root,NO" ]
  [ "$(_bitmap_uuid v1/s2 s1)" = "${s1_uuid}" ]
  [ "$(_nbd_contexts root nbd v1/s2 --previous-snapshot-uuid "${s1_uuid}")" = '["base:allocation"]' ]

  sub_test "A snapshot deleted and created again under the same name gets a new UUID"
  lxc snapshot v1 s1 --bitmap
  s1b_uuid="$(_bitmap_uuid v1 s1)"
  [ "${s1b_uuid}" != "${s1_uuid}" ]
  lxc snapshot v1 s3 --bitmap
  s3_uuid="$(_bitmap_uuid v1 s3)"
  [ "$(_bitmaps v1)" = "$(printf 's1,root,YES\ns2,root,YES\ns3,root,YES')" ]
  [ "$(_bitmaps v1/s3)" = "$(printf 's1,root,NO\ns2,root,NO')" ]
  [ "$(_bitmap_uuid v1/s3 s1)" = "${s1b_uuid}" ]
  [ "$(_bitmap_uuid v1/s3 s2)" = "${s2_uuid}" ]
  [ "$(_nbd_contexts root nbd v1/s3)" = '["base:allocation","qemu:dirty-bitmap:s1","qemu:dirty-bitmap:s2"]' ]
  [ "$(_nbd_contexts root nbd v1/s3 --previous-snapshot-uuid "${s1_uuid}")" = '["base:allocation"]' ]
  [ "$(_nbd_contexts root nbd v1/s3 --previous-snapshot-uuid "${s1b_uuid}")" = '["base:allocation","qemu:dirty-bitmap:s1"]' ]
  [ "$(_nbd_contexts root nbd v1/s3 --previous-snapshot-uuid "${s2_uuid}")" = '["base:allocation","qemu:dirty-bitmap:s2"]' ]

  # Two snapshots in a row without writes give a bitmap with no dirty block.
  _nbd_serve nbd v1/s3 --previous-snapshot-uuid "${s2_uuid}"
  nbdinfo --json --map=qemu:dirty-bitmap:s2 "${NBD_URI}/root" | jq --exit-status 'all(.type == 0)'
  wait "${NBD_PID}"

  sub_test "Renaming a snapshot removes the bitmaps of its old and new names"
  lxc move v1/s2 v1/s2b
  [ "$(_bitmaps v1)" = "$(printf 's1,root,YES\ns3,root,YES')" ]

  # The bitmap s2 of s3 refers to the snapshot now named s2b, whose UUID did not change.
  [ "$(_bitmap_uuid v1/s3 s2)" = "${s2_uuid}" ]
  [ "$(_snapshot_uuid v1/s2b)" = "${s2_uuid}" ]
  [ "$(_nbd_contexts root nbd v1/s3 --previous-snapshot-uuid "${s2_uuid}")" = '["base:allocation","qemu:dirty-bitmap:s2"]' ]

  # The old name can be used again. The bitmap s2 of s3 does not refer to the new s2.
  lxc snapshot v1 s2 --bitmap
  s2b_uuid="$(_bitmap_uuid v1 s2)"
  [ "${s2b_uuid}" != "${s2_uuid}" ]
  [ "$(_bitmaps v1)" = "$(printf 's1,root,YES\ns2,root,YES\ns3,root,YES')" ]
  [ "$(_nbd_contexts root nbd v1/s3 --previous-snapshot-uuid "${s2b_uuid}")" = '["base:allocation"]' ]

  # A name moved to an older snapshot. The listing of s3 keeps the UUID of the snapshot the bitmap was created with.
  lxc delete v1/s2
  lxc move v1/s1 v1/s2
  [ "$(_bitmaps v1)" = "s3,root,YES" ]
  [ "$(_bitmap_uuid v1/s3 s2)" = "${s2_uuid}" ]
  [ "$(_bitmap_uuid v1/s3 s1)" = "${s1b_uuid}" ]
  [ "$(_nbd_contexts root nbd v1/s3 --previous-snapshot-uuid "${s1b_uuid}")" = '["base:allocation","qemu:dirty-bitmap:s1"]' ]
  [ "$(_nbd_contexts root nbd v1/s3 --previous-snapshot-uuid "${s2b_uuid}")" = '["base:allocation"]' ]
  lxc delete v1/s2 v1/s2b v1/p1

  sub_test "An instance snapshot with a bitmap covers the attached block volume"
  # The custom block volume is a raw attached device the guest never writes to on its own, so its content is
  # stable and can be checksummed against the export.
  blk_dev="/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_cbt--blk"
  lxc exec v1 -- sync
  blk_size="$(lxc exec v1 -- blockdev --getsize64 "${blk_dev}")"
  blk_checksum="$(lxc exec v1 -- sha256sum "${blk_dev}" | cut -d' ' -f1)"

  lxc snapshot v1 s4 --bitmap --disk-volumes all-exclusive
  [ "$(_bitmaps v1)" = "$(printf 's3,root,YES\ns4,cbt-blk,YES\ns4,root,YES')" ]
  [ "$(_bitmaps v1/s4)" = "s3,root,NO" ]
  [ "$(lxc bitmap show v1 s4 | yq --exit-status '.volumes | length')" = "2" ]
  [ "$(lxc bitmap show v1 s4 | yq --exit-status '.volumes[0].uuid')" = "${blk_uuid}" ]
  lxc bitmap show v1 s4 | yq --exit-status '.volumes[0].device == "cbt-blk" and .volumes[0].type == "custom" and .volumes[0].name == "cbt-blk"'
  snap_a="$(_volume_snapshot_of s4)"
  [ -n "${snap_a}" ]

  # Every volume of the snapshot is listed under an export named after its disk device, with its own size and
  # its own bitmaps of the snapshot.
  _nbd_serve nbd v1/s4
  nbdinfo --list --json "${NBD_URI}" | jq --exit-status --argjson root "${root_size}" --argjson blk "${blk_size}" '
    ([.exports[] | select(."export-name" == "root")][0] | (."export-size" == $root) and (.contexts == ["base:allocation", "qemu:dirty-bitmap:s3"]))
    and ([.exports[] | select(."export-name" == "cbt-blk")][0] | (."export-size" == $blk) and (.contexts == ["base:allocation"]))'
  wait "${NBD_PID}"

  # Selecting the custom disk export yields the guest's own view of the volume.
  blk_copy="$(mktemp -p "${TEST_DIR}" blk_copy.XXX)"
  _nbd_serve nbd v1/s4
  nbdcopy --connections=1 "${NBD_URI}/cbt-blk" "${blk_copy}"
  wait "${NBD_PID}"
  [ "$(stat -c %s "${blk_copy}")" = "${blk_size}" ]
  [ "$(sha256sum "${blk_copy}" | cut -d' ' -f1)" = "${blk_checksum}" ]
  rm -f "${blk_copy}"

  # The devices filter serves a subset of the volumes.
  _nbd_serve nbd v1/s4 --devices cbt-blk
  nbdinfo --list --json "${NBD_URI}" | jq --exit-status '(.exports | length) == 1 and .exports[0]."export-name" == "cbt-blk"'
  wait "${NBD_PID}"

  sub_test "The next snapshot copies the bitmaps of both volumes"
  lxc snapshot v1 s5 --bitmap --disk-volumes all-exclusive
  snap_b="$(_volume_snapshot_of s5)"
  [ "$(_bitmaps v1/s5)" = "$(printf 's3,root,NO\ns4,cbt-blk,NO\ns4,root,NO')" ]
  [ "$(_nbd_contexts cbt-blk nbd v1/s5)" = '["base:allocation","qemu:dirty-bitmap:s4"]' ]
  [ "$(_nbd_contexts cbt-blk nbd v1/s5 --previous-snapshot-uuid "$(_snapshot_uuid v1/s4)")" = '["base:allocation","qemu:dirty-bitmap:s4"]' ]

  sub_test "Bitmaps are kept across a stop, a start and a reboot"
  # The minimal image runs no logind, so a graceful stop via the ACPI power button never completes.
  lxc stop -f v1
  [ -e "${metadata_images}/${root_uuid}.qcow2" ]
  [ -e "${metadata_images}/${blk_uuid}.qcow2" ]
  [ "$(_bitmaps v1)" = "$(printf 's3,root,YES\ns4,cbt-blk,YES\ns4,root,YES\ns5,cbt-blk,YES\ns5,root,YES')" ]
  [ "$(_bitmap_uuid v1 s3)" = "${s3_uuid}" ]
  lxc start v1
  waitInstanceReady v1
  [ "$(_bitmaps v1)" = "$(printf 's3,root,YES\ns4,cbt-blk,YES\ns4,root,YES\ns5,cbt-blk,YES\ns5,root,YES')" ]
  [ "$(_bitmap_uuid v1 s3)" = "${s3_uuid}" ]

  # A reboot from inside the guest ends the QEMU process, which writes the bitmaps into the images before it exits.
  lxc exec v1 -- systemctl reboot || true
  sleep 5
  waitInstanceReady v1
  [ "$(_bitmaps v1)" = "$(printf 's3,root,YES\ns4,cbt-blk,YES\ns4,root,YES\ns5,cbt-blk,YES\ns5,root,YES')" ]

  # A guest that powers itself off ends the same way.
  lxc exec v1 -- systemctl poweroff || true
  _wait_stopped v1
  lxc start v1
  waitInstanceReady v1
  [ "$(_bitmaps v1)" = "$(printf 's3,root,YES\ns4,cbt-blk,YES\ns4,root,YES\ns5,cbt-blk,YES\ns5,root,YES')" ]

  sub_test "A guest poweroff after an LXD restart keeps the bitmaps"
  # The daemon is killed so that the instance keeps running, and the new daemon handles the shutdown event of the guest.
  kill -9 "$(< "${LXD_DIR}/lxd.pid")"
  respawn_lxd "${LXD_DIR}" true
  [ "$(_bitmaps v1)" = "$(printf 's3,root,YES\ns4,cbt-blk,YES\ns4,root,YES\ns5,cbt-blk,YES\ns5,root,YES')" ]
  lxc exec v1 -- systemctl poweroff || true
  _wait_stopped v1
  lxc start v1
  waitInstanceReady v1
  [ "$(_bitmaps v1)" = "$(printf 's3,root,YES\ns4,cbt-blk,YES\ns4,root,YES\ns5,cbt-blk,YES\ns5,root,YES')" ]

  sub_test "A stopped instance commits an overlay file before its volumes are read"
  # An overlay file left by a failed commit contains guest writes that the volume lacks. The guest never writes to
  # the custom block volume on its own, so the pattern is what it must read back.
  pattern="$(_write_overlay "${blk_uuid}" "${blk_size}")"
  lxc stop -f v1
  [ ! -e "${metadata_images}/${blk_uuid}.overlay.qcow2" ]
  [ "$(_bitmaps v1)" = "$(printf 's3,root,YES\ns4,cbt-blk,YES\ns4,root,YES\ns5,cbt-blk,YES\ns5,root,YES')" ]
  lxc start v1
  waitInstanceReady v1
  [ "$(lxc exec v1 -- head -c 65536 "${blk_dev}" | sha256sum | cut -d' ' -f1)" = "${pattern}" ]
  [ "$(_bitmaps v1)" = "$(printf 's3,root,YES\ns4,cbt-blk,YES\ns4,root,YES\ns5,cbt-blk,YES\ns5,root,YES')" ]

  sub_test "A crash invalidates the bitmaps and the next snapshot with a bitmap removes them"
  # The bitmaps miss the writes after the crash, so they are listed as not recording until a snapshot with a bitmap
  # removes them. The overlay of the crash is committed before the guest runs again.
  lxc exec v1 -- sh -c 'dd if=/dev/urandom of=/root/cbt2.bin bs=1M count=1 && sync'
  pattern="$(_write_overlay "${blk_uuid}" "${blk_size}")"
  pid="$(lxc query /1.0/instances/v1/state | jq --exit-status '.pid')"
  kill -9 "${pid}"
  _wait_stopped v1
  [ "$(_bitmaps v1)" = "$(printf 's3,root,NO\ns4,cbt-blk,NO\ns4,root,NO\ns5,cbt-blk,NO\ns5,root,NO')" ]
  lxc start v1
  waitInstanceReady v1
  [ "$(_bitmaps v1)" = "$(printf 's3,root,NO\ns4,cbt-blk,NO\ns4,root,NO\ns5,cbt-blk,NO\ns5,root,NO')" ]
  [ ! -e "${metadata_images}/${blk_uuid}.overlay.qcow2" ]
  [ "$(lxc exec v1 -- head -c 65536 "${blk_dev}" | sha256sum | cut -d' ' -f1)" = "${pattern}" ]
  lxc snapshot v1 s6 --bitmap --disk-volumes all-exclusive
  [ "$(_bitmaps v1)" = "$(printf 's6,cbt-blk,YES\ns6,root,YES')" ]
  [ "$(_bitmaps v1/s6)" = "" ]
  [ "$(_bitmaps v1/s5)" = "$(printf 's3,root,NO\ns4,cbt-blk,NO\ns4,root,NO')" ]

  sub_test "Restoring a snapshot deletes the bitmaps of the volumes and keeps the copies of the snapshots"
  lxc stop -f v1
  lxc restore v1 s3
  lxc start v1
  waitInstanceReady v1
  [ "$(_bitmaps v1)" = "" ]
  [ "$(_bitmaps v1/s6)" = "" ]
  [ "$(_bitmaps v1/s5)" = "$(printf 's3,root,NO\ns4,cbt-blk,NO\ns4,root,NO')" ]
  ! lxc exec v1 -- test -e /root/cbt2.bin || false

  sub_test "A snapshot that is exported cannot be deleted or renamed"
  _nbd_serve nbd v1/s5
  _nbd_hold
  [ "$(! "${_LXC}" delete v1/s5 2>&1 1>/dev/null)" = 'Error: Snapshot "v1/s5" is exported over NBD: In use' ]
  [ "$(! "${_LXC}" move v1/s5 v1/s5-renamed 2>&1 1>/dev/null)" = 'Error: Snapshot "v1/s5" is exported over NBD: In use' ]
  [ "$(! "${_LXC}" storage volume snapshot delete "${pool}" cbt-blk "${snap_b}" 2>&1 1>/dev/null)" = "Error: Snapshot \"cbt-blk/${snap_b}\" is exported over NBD: In use" ]
  _nbd_release

  sub_test "Deleting a custom volume snapshot removes the bitmap created with it from that volume"
  lxc snapshot v1 s7 --bitmap --disk-volumes all-exclusive
  lxc snapshot v1 s8 --bitmap --disk-volumes all-exclusive
  [ "$(_bitmaps v1)" = "$(printf 's7,cbt-blk,YES\ns7,root,YES\ns8,cbt-blk,YES\ns8,root,YES')" ]
  [ "$(_bitmaps v1/s8)" = "$(printf 's7,cbt-blk,NO\ns7,root,NO')" ]
  lxc storage volume snapshot delete "${pool}" cbt-blk "$(_volume_snapshot_of s7)"
  [ "$(_bitmaps v1)" = "$(printf 's7,root,YES\ns8,cbt-blk,YES\ns8,root,YES')" ]

  # The export of s8 keeps the copies of both volumes, as the snapshot metadata images are not modified.
  [ "$(_nbd_contexts cbt-blk nbd v1/s8 --previous-snapshot-uuid "$(_snapshot_uuid v1/s7)")" = '["base:allocation","qemu:dirty-bitmap:s7"]' ]

  # The next snapshot copies the bitmap of the root disk only.
  lxc snapshot v1 s9 --bitmap --disk-volumes all-exclusive
  [ "$(_bitmaps v1/s9)" = "$(printf 's7,root,NO\ns8,cbt-blk,NO\ns8,root,NO')" ]

  # Renaming a custom volume snapshot keeps the bitmap, as the snapshot keeps its UUID.
  lxc storage volume rename "${pool}" "cbt-blk/$(_volume_snapshot_of s8)" cbt-blk/s8-renamed
  [ "$(_bitmaps v1)" = "$(printf 's7,root,YES\ns8,cbt-blk,YES\ns8,root,YES\ns9,cbt-blk,YES\ns9,root,YES')" ]
  [ "$(_nbd_contexts cbt-blk nbd v1/s9)" = '["base:allocation","qemu:dirty-bitmap:s8"]' ]

  sub_test "Deleting the custom volume snapshot of an exported snapshot leaves the export without that volume"
  # The export is released after its relay ends, so the first delete attempt can race it.
  for _ in $(seq 10); do
    lxc storage volume snapshot delete "${pool}" cbt-blk "${snap_b}" && break
    sleep 1
  done
  _nbd_serve nbd v1/s5
  nbdinfo --list --json "${NBD_URI}" | jq --exit-status '(.exports | length) == 1 and .exports[0]."export-name" == "root"'
  wait "${NBD_PID}"
  [ "$(_bitmaps v1/s5)" = "$(printf 's3,root,NO\ns4,root,NO')" ]
  lxc delete v1/s5 v1/s6 v1/s7 v1/s8 v1/s9
  lxc storage volume snapshot delete "${pool}" cbt-blk "${snap_a}"
  lxc storage volume snapshot delete "${pool}" cbt-blk s8-renamed
  [ "$(_bitmaps v1)" = "" ]

  sub_test "Renaming a device keeps the bitmaps of its volume"
  lxc snapshot v1 s10 --bitmap --disk-volumes all-exclusive
  [ "$(_bitmaps v1)" = "$(printf 's10,cbt-blk,YES\ns10,root,YES')" ]
  lxc config show v1 | sed 's/^  cbt-blk:$/  cbt-blk2:/' | lxc config edit v1
  [ "$(_bitmaps v1)" = "$(printf 's10,cbt-blk2,YES\ns10,root,YES')" ]
  [ -e "${metadata_images}/${blk_uuid}.qcow2" ]
  lxc stop -f v1
  lxc config show v1 | sed 's/^  cbt-blk2:$/  cbt-blk:/' | lxc config edit v1
  [ "$(_bitmaps v1)" = "$(printf 's10,cbt-blk,YES\ns10,root,YES')" ]
  lxc start v1
  waitInstanceReady v1
  [ "$(_bitmaps v1)" = "$(printf 's10,cbt-blk,YES\ns10,root,YES')" ]

  # The export of a snapshot taken before the rename names the export after the device name of the time.
  [ "$(_nbd_contexts cbt-blk nbd v1/s10)" = '["base:allocation"]' ]

  sub_test "Containers, stopped virtual machines and shared volumes get no bitmaps"
  ensure_import_testimage
  lxc launch testimage c1
  [ "$(! "${_LXC}" snapshot c1 --bitmap 2>&1 1>/dev/null)" = "Error: A snapshot with a bitmap requires a running virtual machine" ]
  lxc delete -f c1

  lxc stop -f v1
  [ "$(! "${_LXC}" snapshot v1 --bitmap 2>&1 1>/dev/null)" = "Error: A snapshot with a bitmap requires a running virtual machine" ]
  lxc snapshot v1 p2
  [ "$(_bitmaps v1)" = "$(printf 's10,cbt-blk,YES\ns10,root,YES')" ]
  lxc delete v1/p2
  lxc start v1
  waitInstanceReady v1

  lxc storage volume create "${pool}" cbt-shared size=32MiB --type block security.shared=true
  lxc storage volume attach "${pool}" cbt-shared v1
  lxc snapshot v1 s11 --bitmap --disk-volumes all-exclusive
  [ "$(_bitmaps v1)" = "$(printf 's10,cbt-blk,YES\ns10,root,YES\ns11,cbt-blk,YES\ns11,root,YES')" ]
  [ "$(find "${metadata_images}" -name "$(lxc storage volume get "${pool}" cbt-shared volatile.uuid).qcow2" | wc -l)" = "0" ]
  lxc storage volume detach "${pool}" cbt-shared v1
  lxc storage volume delete "${pool}" cbt-shared

  sub_test "Enabling security.shared deletes the bitmaps of the volume"
  lxc storage volume set "${pool}" cbt-blk security.shared=true
  [ "$(_bitmaps v1)" = "$(printf 's10,root,YES\ns11,root,YES')" ]
  lxc storage volume unset "${pool}" cbt-blk security.shared
  lxc snapshot v1 s12 --bitmap --disk-volumes all-exclusive
  [ "$(_bitmaps v1)" = "$(printf 's10,root,YES\ns11,root,YES\ns12,cbt-blk,YES\ns12,root,YES')" ]

  sub_test "A writable NBD export of a stopped volume deletes its bitmaps"
  lxc stop -f v1
  [ "$(_bitmaps v1)" = "$(printf 's10,root,YES\ns11,root,YES\ns12,cbt-blk,YES\ns12,root,YES')" ]

  # The export is read-write, which the command requires a confirmation of.
  [ "$(! "${_LXC}" storage volume nbd "${pool}" cbt-blk 2>&1 1>/dev/null)" = "Error: The volume is served read-write, which --writable confirms" ]

  import_src="$(mktemp -p "${TEST_DIR}" import_src.XXX)"
  head -c "${blk_size}" /dev/urandom > "${import_src}"
  _nbd_serve storage volume nbd "${pool}" cbt-blk --writable
  nbdinfo --json "${NBD_URI}" | jq --exit-status --argjson size "${blk_size}" '.exports[0] | (.is_read_only == false) and (."export-size" == $size)'
  nbdcopy --connections=1 "${import_src}" "${NBD_URI}"
  wait "${NBD_PID}"
  [ "$(_bitmaps v1)" = "$(printf 's10,root,YES\ns11,root,YES\ns12,root,YES')" ]

  # The instance cannot start while a session writes one of its volumes.
  _nbd_serve storage volume nbd "${pool}" virtual-machine/v1 --writable
  _nbd_hold
  ! lxc start v1 || false
  _nbd_release

  _nbd_serve storage volume nbd "${pool}" virtual-machine/v1 --writable
  nbdcopy --connections=1 "${s1_copy}" "${NBD_URI}"
  wait "${NBD_PID}"
  [ "$(_bitmaps v1)" = "" ]

  # The volume is released after the export's relay ends, so the first start attempt can race it.
  for _ in $(seq 10); do
    lxc start v1 && break
    sleep 1
  done
  waitInstanceReady v1
  ! lxc exec v1 -- test -e /root/cbt.bin || false
  [ "$(lxc exec v1 -- sha256sum "${blk_dev}" | cut -d' ' -f1)" = "$(sha256sum "${import_src}" | cut -d' ' -f1)" ]
  rm -f "${import_src}"

  # An export is refused while the instance runs.
  _nbd_serve storage volume nbd "${pool}" virtual-machine/v1 --writable
  ! nbdinfo "${NBD_URI}" || false
  ! wait "${NBD_PID}" || false
  [[ "$(cat "${NBD_STDERR}")" == "Error: NBD export requires the instance to be stopped"* ]]
  [ "$(! "${_LXC}" nbd v1 2>&1 1>/dev/null)" = "Error: Missing instance snapshot name" ]

  sub_test "A copy and an imported instance have no bitmaps"
  lxc snapshot v1 s13 --bitmap
  [ "$(_bitmaps v1)" = "s13,root,YES" ]
  lxc stop -f v1

  # The metadata images of v1 record neither the writes to v2 nor its snapshots, so v2 starts with new images.
  lxc copy v1 v2
  lxc start v2
  waitInstanceReady v2
  [ "$(_bitmaps v2)" = "" ]
  prepare_vm_for_hard_stop v2
  lxc delete -f v2

  # The backup of the stopped v1 contains its metadata images. The lxc wrapper kills its command after 120s, which
  # the export of a root disk can exceed, so the binary is called directly.
  "${_LXC}" export v1 "${TEST_DIR}/v1.tar.gz" --instance-only
  "${_LXC}" import "${TEST_DIR}/v1.tar.gz" v3
  rm -f "${TEST_DIR}/v1.tar.gz"
  lxc start v3
  waitInstanceReady v3
  [ "$(_bitmaps v3)" = "" ]
  prepare_vm_for_hard_stop v3
  lxc delete -f v3

  # The copy and the export kept the metadata images of v1.
  lxc start v1
  waitInstanceReady v1
  [ "$(_bitmaps v1)" = "s13,root,YES" ]

  sub_test "A resize and a detach delete the bitmaps of the volume"
  lxc snapshot v1 s14 --bitmap --disk-volumes all-exclusive
  [ "$(_bitmaps v1)" = "$(printf 's13,root,YES\ns14,cbt-blk,YES\ns14,root,YES')" ]
  lxc stop -f v1
  lxc storage volume set "${pool}" cbt-blk size=64MiB
  [ "$(_bitmaps v1)" = "$(printf 's13,root,YES\ns14,root,YES')" ]
  lxc config device set v1 root size=5GiB
  [ "$(_bitmaps v1)" = "" ]
  lxc start v1
  waitInstanceReady v1

  # The images were created again at the new sizes.
  [ "$(lxc exec v1 -- blockdev --getsize64 "${blk_dev}")" = "$((64 * 1024 * 1024))" ]
  lxc snapshot v1 s15 --bitmap --disk-volumes all-exclusive
  [ "$(_bitmaps v1)" = "$(printf 's15,cbt-blk,YES\ns15,root,YES')" ]
  lxc stop -f v1
  [ -e "${metadata_images}/${blk_uuid}.qcow2" ]
  lxc storage volume detach "${pool}" cbt-blk v1
  [ ! -e "${metadata_images}/${blk_uuid}.qcow2" ]
  [ "$(_bitmaps v1)" = "s15,root,YES" ]

  # A volume attached again starts without bitmaps and with a new image.
  lxc storage volume attach "${pool}" cbt-blk v1
  lxc start v1
  waitInstanceReady v1
  [ -e "${metadata_images}/${blk_uuid}.qcow2" ]
  [ "$(_bitmaps v1)" = "s15,root,YES" ]

  # Cleanup.
  rm -f "${s1_copy}" "${s3_copy}" "${TEST_DIR}"/nbd_stderr.*
  lxc delete -f v1
  lxc storage volume delete "${pool}" cbt-blk

  if [ -n "${orig_volume_size:-}" ]; then
    # Restore the volume.size.
    lxc storage set "${pool}" volume.size "${orig_volume_size}"
  fi
}
