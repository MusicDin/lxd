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

# _nbd_sessions prints the IDs of the running operations of NBD sessions, sorted.
_nbd_sessions() {
  lxc operation list --format json | jq --raw-output '.[] | select((.description == "Exporting instance snapshot over NBD" or .description == "Importing storage volume over NBD") and .status == "Running") | .id' | sort
}

# _nbd_hold keeps an NBD session of the export at NBD_URI open until _nbd_release runs. nc ignores its stdin so
# that it exits once the server closes the connection. The export command contacts LXD only once the client
# connects, and LXD lists the operation of the session once the volumes are mounted and the export holds them, so
# the helper waits for an operation that was not running before it connected.
_nbd_hold() {
  local address before
  before="$(_nbd_sessions)"
  address="${NBD_URI#nbd://}"
  nc -d "${address%:*}" "${address##*:}" &
  NBD_HOLDER_PID=$!
  for _ in $(seq 60); do
    [ -n "$(comm -13 <(echo "${before}") <(_nbd_sessions))" ] && return 0
    sleep 0.5
  done

  return 1
}

# _nbd_release ends the session opened by _nbd_hold and waits for the export command to exit.
_nbd_release() {
  kill "${NBD_HOLDER_PID}" 2>/dev/null || true
  wait "${NBD_HOLDER_PID}" || true
  wait "${NBD_PID}" || true
}

# _nbd_wait_imports waits for the writable NBD sessions to end on the server, which releases a volume only after the
# relay of its session ends and the volume is flushed and deactivated, so that a new session or a start of the
# instance does not race it. The flush of a root volume written in full takes a while.
_nbd_wait_imports() {
  for _ in $(seq 120); do
    lxc operation list --format json | jq --exit-status '[.[] | select(.description == "Importing storage volume over NBD" and .status == "Running")] | length == 0' > /dev/null && return 0
    sleep 1
  done

  return 1
}

# _nbd_contexts prints the metadata contexts of the named export of the snapshot NBD command given as the remaining
# arguments, as a JSON array, and waits for the command to exit.
_nbd_contexts() {
  local export_name="$1"
  shift
  _nbd_serve "$@"
  nbdinfo --list --json "${NBD_URI}" | jq --exit-status --compact-output --arg name "${export_name}" '[.exports[] | select(."export-name" == $name)][0].contexts'
  wait "${NBD_PID}"
}

# _bitmaps prints the bitmaps of an instance or of an instance snapshot as "name,device,recording" lines, sorted.
_bitmaps() {
  lxc bitmap list "$@" --format csv -c ndr | sort
}

# _bitmap_uuid prints the UUID of the named bitmap of an instance or of an instance snapshot.
_bitmap_uuid() {
  lxc bitmap show "$1" "$2" | yq -r --exit-status '.uuid'
}

# _snapshot_uuid prints the instance snapshot UUID of a snapshot, the UUID of its root volume snapshot.
_snapshot_uuid() {
  lxc storage volume get "${pool}" "virtual-machine/$1" volatile.uuid
}

# _volume_snapshot_of prints the name of the snapshot of the custom volume cbt-blk that was taken with the given
# instance snapshot, which the instance snapshot records in volatile.attached_volumes.
_volume_snapshot_of() {
  local uuid
  uuid="$(lxc config show "v1/$1" | yq -r --exit-status '.config."volatile.attached_volumes"' | jq --raw-output '."cbt-blk"')"
  lxc query "/1.0/storage-pools/${pool}/volumes/custom/cbt-blk/snapshots?recursion=1" | jq --exit-status --raw-output --arg uuid "${uuid}" '.[] | select(.config."volatile.uuid" == $uuid) | .name | ltrimstr("cbt-blk/")'
}

# _wait_stopped waits for the stop hook of the instance to end. The state is STOPPED between the end of the QEMU
# process and the start of the hook too, and RUNNING while the hook holds the stop lock, so the hook is known to have
# ended once it has set volatile.last_state.power and the state is STOPPED after that.
_wait_stopped() {
  for _ in $(seq 60); do
    [ "$(lxc config get "$1" volatile.last_state.power)" = "STOPPED" ] && [ "$(lxc list -f csv -c s "$1")" = "STOPPED" ] && return 0
    sleep 1
  done

  return 1
}

# _write_overlay creates an overlay file for the volume of the given UUID on the live config volume of v1, with the
# overlay bitmap that a snapshot with a bitmap adds and a 64 KiB pattern written at its start, as a failed commit
# leaves one. It prints the checksum of the pattern.
_write_overlay() {
  local overlay="${metadata_images}/$1.overlay.qcow2"
  qemu-img create -f qcow2 "${overlay}" "$2" > /dev/null
  qemu-img bitmap --add -g 65536 -f qcow2 "${overlay}" writes
  qemu-io -f qcow2 -c "write -P 0xab 0 64k" "${overlay}" > /dev/null
  head -c 65536 /dev/zero | tr '\0' '\253' | sha256sum | cut -d' ' -f1
}

# _metadata_image_info prints the data-file-raw flag, the allocated size and the bitmaps with their flags of the
# metadata image at the given path, as JSON. The image records a data file that no longer exists, so a null block
# device stands in for it.
_metadata_image_info() {
  qemu-img info --force-share --output=json --image-opts "driver=qcow2,file.filename=$1,data-file.driver=null-co" | jq --exit-status --compact-output '{raw: ."format-specific".data."data-file-raw", size: ."actual-size", bitmaps: [(."format-specific".data.bitmaps // [])[] | {name, flags}]}'
}

# _qemu_monitor runs the given QMP commands on the QEMU process of v1 over its monitor socket and prints the
# responses. The socket serves one client at a time, so it is used while LXD is not running.
_qemu_monitor() {
  {
    echo '{"execute":"qmp_capabilities"}'
    printf '%s\n' "$@"
  } | nc -U -q 1 "${LXD_DIR}/logs/v1/qemu.monitor"
}

# _qemu_run_state prints the run state of the QEMU process of v1.
_qemu_run_state() {
  _qemu_monitor '{"execute":"query-status"}' | jq --raw-output --exit-status 'select(.return.status != null) | .return.status'
}

# _mount_config_snapshot mounts the config volume snapshot of the given snapshot of v1 read-only at the given
# directory, through the storage backend, so that the metadata images the snapshot was taken with can be read. It
# fails on a backend it does not know. _umount_config_snapshot undoes it.
_mount_config_snapshot() {
  mkdir -p "$2"
  case "${LXD_BACKEND}" in
    lvm)
      lvchange -ay -K "${pool}/virtual-machines_v1-$1"
      mount -o ro "/dev/${pool}/virtual-machines_v1-$1" "$2"
      ;;
    zfs)
      mount -t zfs -o ro "${pool}/virtual-machines/v1@snapshot-$1" "$2"
      ;;
    dir|btrfs)
      mount --bind -o ro "${LXD_DIR}/storage-pools/${pool}/virtual-machines-snapshots/v1/$1" "$2"
      ;;
    *)
      rmdir "$2"
      return 1
      ;;
  esac
}

_umount_config_snapshot() {
  umount "$2"
  rmdir "$2"
  if [ "${LXD_BACKEND}" = "lvm" ]; then
    lvchange -an "${pool}/virtual-machines_v1-$1"
  fi
}

test_storage_block_tracking_vm() {
  if ! check_dependencies nbdinfo nbdcopy qemu-img qemu-io qemu-nbd qemu-storage-daemon; then
    export TEST_UNMET_REQUIREMENT="Missing nbdinfo, nbdcopy, qemu-img, qemu-io, qemu-nbd or qemu-storage-daemon"
    return
  fi

  local pool orig_volume_size root_dev root_size s1_uuid s1b_uuid s2_uuid s2b_uuid s3_uuid s1_copy s3_copy reconstructed extents offset length chunk address blk_dev blk_size blk_checksum blk_copy operation_uuid pid new_pid metadata_images root_uuid blk_uuid pattern import_src snap_a snap_b bitmap_file data_uuid data_dev shared_uuid part_start first_block file_offset
  pool="lxdtest-$(basename "${LXD_DIR}")"
  orig_volume_size="$(lxc storage get "${pool}" volume.size)"
  if [ -n "${orig_volume_size:-}" ]; then
    # Override the volume.size to accommodate a VM
    lxc storage set "${pool}" volume.size "${SMALLEST_VM_ROOT_DISK}"
  fi

  # The writable NBD import of the root disk and the import of the backup each write the whole root disk, which
  # allocates it in full, and the pool of the harness cannot hold two such disks beside the image.
  if [ -n "$(lxc storage get "${pool}" size)" ]; then
    lxc storage set "${pool}" size=10GiB
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

  sub_test "Every block disk has a volume metadata image"
  # The images exist from the first start on, before any bitmap is created, and nothing else is in the directory.
  # An image stores bitmaps only, so it has no preallocated tables and takes a few hundred KiB.
  [ -e "${metadata_images}/${root_uuid}.qcow2" ]
  [ -e "${metadata_images}/${blk_uuid}.qcow2" ]
  [ "$(find "${metadata_images}" -name '*.qcow2' | wc -l)" = "2" ]
  [ "$(_bitmaps v1 || echo fail)" = "" ]
  _metadata_image_info "${metadata_images}/${root_uuid}.qcow2" | jq --exit-status '.raw == false and .size < 1048576 and .bitmaps == []'

  sub_test "A snapshot with a bitmap creates the bitmap on the root disk"
  lxc snapshot v1 s1 --bitmap
  [ "$(_bitmaps v1)" = "s1,root,YES" ]
  lxc bitmap show v1 s1 | yq --exit-status '.name == "s1" and (.volumes | length) == 1 and .volumes[0].device == "root" and .volumes[0].type == "virtual-machine" and .volumes[0].name == "v1" and .volumes[0].recording == true and .volumes[0].granularity == 65536'
  [ "$(lxc bitmap show v1 s1 | yq -r --exit-status '.volumes[0].uuid')" = "${root_uuid}" ]
  [ "$(lxc bitmap show v1 s1 | yq -r --exit-status '.volumes[0].pool')" = "${pool}" ]
  [ "$(! "${_LXC}" bitmap show v1 missing 2>&1 1>/dev/null)" = "Error: Bitmap not found" ]

  # The UUID of the bitmap is the instance snapshot UUID of the snapshot it was created with.
  s1_uuid="$(_bitmap_uuid v1 s1)"
  [ "${s1_uuid}" = "$(_snapshot_uuid v1/s1)" ]

  # The snapshot bitmap file is removed from the config volume once the config volume snapshot includes it, so the
  # images are the only files there.
  [ "$(find "${metadata_images}" -mindepth 1 -printf '%f\n' | sort)" = "$(printf '%s.qcow2\n%s.qcow2' "${blk_uuid}" "${root_uuid}" | sort)" ]

  # A snapshot of a name that exists is rejected when the request is received.
  [ "$(! "${_LXC}" snapshot v1 s1 --bitmap 2>&1 1>/dev/null)" = 'Error: Snapshot "s1" already exists' ]
  [ "$(! "${_LXC}" snapshot v1 s1 2>&1 1>/dev/null)" = 'Error: Snapshot "s1" already exists' ]

  # The image of the first snapshot with a bitmap holds its own bitmap only, so its export offers the allocation map only.
  [ "$(_bitmaps v1/s1 || echo fail)" = "" ]
  [ "$(_nbd_contexts root nbd v1/s1)" = '["base:allocation"]' ]

  sub_test "A snapshot without a bitmap keeps the bitmaps and has no export"
  lxc snapshot v1 p1
  [ "$(_bitmaps v1)" = "s1,root,YES" ]
  [ "$(_bitmaps v1/p1 || echo fail)" = "" ]
  _nbd_serve nbd v1/p1
  ! nbdinfo "${NBD_URI}" || false
  ! wait "${NBD_PID}" || false
  [[ "$(cat "${NBD_STDERR}")" == "Error: Snapshot was not created with a bitmap"* ]]

  # The config volume snapshot holds the live images, whose bitmaps are in use, and no snapshot bitmap file.
  if _mount_config_snapshot p1 "${TEST_DIR}/p1-config"; then
    _metadata_image_info "${TEST_DIR}/p1-config/metadata_images/${root_uuid}.qcow2" | jq --exit-status '.bitmaps == [{"name": "s1", "flags": ["in-use"]}]'
    [ "$(find "${TEST_DIR}/p1-config/metadata_images" -name 'snapshot.*.yaml' | wc -l)" = "0" ]
    _umount_config_snapshot p1 "${TEST_DIR}/p1-config"
  else
    echo "==> Skipping the check of the snapshot images on the ${LXD_BACKEND} backend"
  fi

  sub_test "A leftover snapshot bitmap file is ignored and pruned"
  # A file that a failed snapshot left on the config volume records no snapshot that exists, so a snapshot taken
  # over it is not one created with a bitmap, and the next start removes it.
  cat > "${metadata_images}/snapshot.stale.yaml" << EOF
snapshot:
  uuid: 00000000-0000-4000-8000-000000000001
  name: stale
volumes:
  root:
    uuid: ${root_uuid}
    bitmaps: []
EOF
  lxc snapshot v1 p2
  [ "$(_bitmaps v1/p2 || echo fail)" = "" ]
  _nbd_serve nbd v1/p2
  ! nbdinfo "${NBD_URI}" || false
  ! wait "${NBD_PID}" || false
  [[ "$(cat "${NBD_STDERR}")" == "Error: Snapshot was not created with a bitmap"* ]]
  lxc delete v1/p2
  lxc stop -f v1
  lxc start v1
  waitInstanceReady v1
  [ ! -e "${metadata_images}/snapshot.stale.yaml" ]
  [ "$(_bitmaps v1)" = "s1,root,YES" ]

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
  [ "$(! "${_LXC}" bitmap show v1/missing s1 2>&1 1>/dev/null)" = 'Error: Failed fetching snapshot "missing" of instance "v1" in project "default": InstanceSnapshot not found' ]

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

  # Every export serves a single client, so each extent is read in a session of its own.
  cp "${s1_copy}" "${reconstructed}"
  _nbd_serve nbd v1/s2 --previous-snapshot-uuid "${s1_uuid}"
  extents="$(nbdinfo --json --map=qemu:dirty-bitmap:s1 "${NBD_URI}/root" | jq --exit-status --compact-output '[.[] | select(.type == 1) | [.offset, .length]]')"
  wait "${NBD_PID}"
  [ "$(echo "${extents}" | jq --exit-status 'length')" -gt 0 ]
  chunk="$(mktemp -p "${TEST_DIR}" chunk.XXX)"
  while read -r offset length; do
    _nbd_serve nbd v1/s2
    address="${NBD_URI#nbd://}"
    qemu-img convert --image-opts "driver=raw,offset=${offset},size=${length},file.driver=nbd,file.server.type=inet,file.server.host=${address%:*},file.server.port=${address##*:},file.export=root" -O raw "${chunk}"
    wait "${NBD_PID}"
    dd if="${chunk}" of="${reconstructed}" bs=64K seek="$((offset / 65536))" conv=notrunc status=none
  done < <(echo "${extents}" | jq --exit-status --raw-output '.[] | "\(.[0]) \(.[1])"')
  rm -f "${chunk}"

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
  operation_uuid="$(lxc operation list --format json | jq --exit-status --raw-output '[.[] | select(.description == "Exporting instance snapshot over NBD" and .status == "Running")][0].id')"
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

  # A file written between the two snapshots, whose first block lies at the partition start of the root
  # filesystem plus its first physical block.
  lxc exec v1 -- sh -c 'head -c 65536 /dev/urandom > /root/cbt-s3.bin && sync'
  part_start="$(lxc exec v1 -- sh -c "cat \"/sys/class/block/\$(basename \"\$(findmnt -no SOURCE /)\")/start\"")"
  first_block="$(lxc exec v1 -- filefrag -v -b4096 /root/cbt-s3.bin | awk '/^ *0:/ { sub(/\.\./, "", $4); print $4 }')"
  [ -n "${part_start}" ]
  [ -n "${first_block}" ]
  file_offset="$((part_start * 512 + first_block * 4096))"
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

  # The guest writes to its root disk on its own, so the bitmap is checked for the block of the file only. That a
  # bitmap without writes has no dirty block is checked on a custom volume in "Every attached custom volume has its
  # own bitmaps".
  _nbd_serve nbd v1/s3 --previous-snapshot-uuid "${s1b_uuid}"
  nbdinfo --json --map=qemu:dirty-bitmap:s1 "${NBD_URI}/root" | jq --exit-status --argjson offset "${file_offset}" 'any(.[]; .type == 1 and .offset <= $offset and $offset < .offset + .length)'
  wait "${NBD_PID}"

  sub_test "Renaming a snapshot removes the bitmaps of its old and new names"
  lxc move v1/s2 v1/s2b
  [ "$(_bitmaps v1)" = "$(printf 's1,root,YES\ns3,root,YES')" ]

  # The bitmap s2 of s3 refers to the snapshot now named s2b, whose UUID did not change.
  [ "$(_bitmap_uuid v1/s3 s2)" = "${s2_uuid}" ]
  [ "$(_snapshot_uuid v1/s2b)" = "${s2_uuid}" ]
  [ "$(_nbd_contexts root nbd v1/s3 --previous-snapshot-uuid "${s2_uuid}")" = '["base:allocation","qemu:dirty-bitmap:s2"]' ]

  # The renamed snapshot is listed and exported from the file of its name at creation, which its UUID identifies.
  # Not on zfs, where the mount of a snapshot renamed since the last snapdev=hidden can wait for its zvol until the
  # deadline, because udev exposes the zvol under the old name of the snapshot.
  if [ "${LXD_BACKEND}" != "zfs" ]; then
    [ "$(_bitmaps v1/s2b)" = "s1,root,NO" ]
    [ "$(_nbd_contexts root nbd v1/s2b)" = '["base:allocation","qemu:dirty-bitmap:s1"]' ]
  fi

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
  [ "$(lxc bitmap show v1 s4 | yq -r --exit-status '.volumes[0].uuid')" = "${blk_uuid}" ]
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

  sub_test "The snapshot bitmap file lists the disks the snapshot handled"
  # A snapshot of the root disk alone creates no bitmap on the attached volume, so the file has an entry for the root
  # disk only and the export has no volume for the device of the attached volume. The image of that volume in the
  # config volume snapshot is the live image, with its bitmaps in use.
  lxc snapshot v1 s5a --bitmap
  [ "$(_bitmaps v1)" = "$(printf 's3,root,YES\ns4,cbt-blk,YES\ns4,root,YES\ns5,cbt-blk,YES\ns5,root,YES\ns5a,root,YES')" ]
  [ "$(_bitmaps v1/s5a)" = "$(printf 's3,root,NO\ns4,root,NO\ns5,root,NO')" ]
  _nbd_serve nbd v1/s5a --devices cbt-blk
  ! nbdinfo "${NBD_URI}" || false
  ! wait "${NBD_PID}" || false
  [[ "$(cat "${NBD_STDERR}")" == 'Error: Snapshot has no volume snapshot with bitmaps for device "cbt-blk"'* ]]
  if _mount_config_snapshot s5a "${TEST_DIR}/s5a-config"; then
    [ "$(yq -r --exit-status '.volumes | keys | join(",")' < "${TEST_DIR}/s5a-config/metadata_images/snapshot.s5a.yaml")" = "root" ]
    _metadata_image_info "${TEST_DIR}/s5a-config/metadata_images/${blk_uuid}.qcow2" | jq --exit-status '(.bitmaps | sort_by(.name)) == [{"name": "s4", "flags": ["in-use"]}, {"name": "s5", "flags": ["in-use"]}]'
    _umount_config_snapshot s5a "${TEST_DIR}/s5a-config"
  fi
  lxc delete v1/s5a
  [ "$(_bitmaps v1)" = "$(printf 's3,root,YES\ns4,cbt-blk,YES\ns4,root,YES\ns5,cbt-blk,YES\ns5,root,YES')" ]

  sub_test "The image of a snapshot holds the copies it was written with"
  # QEMU writes the bitmaps into the volume metadata image when the store node closes, and LXD reads every image
  # back before the storage snapshot, so the config volume snapshot holds every bitmap and none is in use. The
  # snapshot bitmap file next to the images records the snapshot and the bitmaps each image holds, without the one
  # created with the snapshot.
  if _mount_config_snapshot s5 "${TEST_DIR}/s5-config"; then
    _metadata_image_info "${TEST_DIR}/s5-config/metadata_images/${root_uuid}.qcow2" | jq --exit-status '(.bitmaps | sort_by(.name)) == [{"name": "s3", "flags": []}, {"name": "s4", "flags": []}, {"name": "s5", "flags": []}]'
    _metadata_image_info "${TEST_DIR}/s5-config/metadata_images/${blk_uuid}.qcow2" | jq --exit-status '(.bitmaps | sort_by(.name)) == [{"name": "s4", "flags": []}, {"name": "s5", "flags": []}]'
    bitmap_file="${TEST_DIR}/s5-config/metadata_images/snapshot.s5.yaml"
    [ "$(yq -r --exit-status '.snapshot.uuid' < "${bitmap_file}")" = "$(_snapshot_uuid v1/s5)" ]
    [ "$(yq -r --exit-status '.snapshot.name' < "${bitmap_file}")" = "s5" ]
    [ "$(yq -r --exit-status '.volumes | keys | join(",")' < "${bitmap_file}")" = "cbt-blk,root" ]
    [ "$(yq -r --exit-status '.volumes.root.uuid' < "${bitmap_file}")" = "${root_uuid}" ]
    [ "$(yq -r --exit-status '.volumes.root.bitmaps | map(.name + ":" + .uuid) | join(",")' < "${bitmap_file}")" = "s3:${s3_uuid},s4:$(_snapshot_uuid v1/s4)" ]
    [ "$(yq -r --exit-status '.volumes.root.bitmaps | map(.granularity == 65536) | all' < "${bitmap_file}")" = "true" ]
    [ "$(yq -r --exit-status '.volumes."cbt-blk".uuid' < "${bitmap_file}")" = "${blk_uuid}" ]
    [ "$(yq -r --exit-status '.volumes."cbt-blk".bitmaps | map(.name + ":" + .uuid) | join(",")' < "${bitmap_file}")" = "s4:$(_snapshot_uuid v1/s4)" ]
    [ "$(yq -r --exit-status '.volumes."cbt-blk".bitmaps | map(.granularity == 65536) | all' < "${bitmap_file}")" = "true" ]
    # The overlays of the disks are on the config volume until the storage snapshot is taken, so the config volume
    # snapshot holds them next to the images and the file.
    [ "$(find "${TEST_DIR}/s5-config/metadata_images" -mindepth 1 -printf '%f\n' | sort)" = "$(printf '%s.overlay.qcow2\n%s.qcow2\n%s.overlay.qcow2\n%s.qcow2\nsnapshot.s5.yaml' "${blk_uuid}" "${blk_uuid}" "${root_uuid}" "${root_uuid}" | sort)" ]
    _umount_config_snapshot s5 "${TEST_DIR}/s5-config"
  else
    echo "==> Skipping the check of the snapshot images on the ${LXD_BACKEND} backend"
  fi

  sub_test "The config volume holds each bitmap once"
  # The images are written for the config volume snapshot and the store nodes are added back once the overlays are
  # committed, which marks the bitmaps in use again, so the live config volume holds one image per disk and nothing
  # else.
  [ "$(find "${metadata_images}" -mindepth 1 -printf '%f\n' | sort)" = "$(printf '%s.qcow2\n%s.qcow2' "${blk_uuid}" "${root_uuid}" | sort)" ]
  _metadata_image_info "${metadata_images}/${root_uuid}.qcow2" | jq --exit-status '(.bitmaps | sort_by(.name)) == [{"name": "s3", "flags": ["in-use"]}, {"name": "s4", "flags": ["in-use"]}, {"name": "s5", "flags": ["in-use"]}]'
  _metadata_image_info "${metadata_images}/${blk_uuid}.qcow2" | jq --exit-status '(.bitmaps | sort_by(.name)) == [{"name": "s4", "flags": ["in-use"]}, {"name": "s5", "flags": ["in-use"]}]'

  sub_test "Bitmaps are kept across a stop, a start and a reboot"
  # The minimal image runs no logind, so a graceful stop via the ACPI power button never completes.
  lxc stop -f v1
  [ "$(_bitmaps v1)" = "$(printf 's3,root,YES\ns4,cbt-blk,YES\ns4,root,YES\ns5,cbt-blk,YES\ns5,root,YES')" ]
  [ "$(_bitmap_uuid v1 s3)" = "${s3_uuid}" ]
  lxc start v1
  waitInstanceReady v1
  [ "$(_bitmaps v1)" = "$(printf 's3,root,YES\ns4,cbt-blk,YES\ns4,root,YES\ns5,cbt-blk,YES\ns5,root,YES')" ]
  [ "$(_bitmap_uuid v1 s3)" = "${s3_uuid}" ]

  # A reboot from inside the guest pauses the QEMU process, and LXD merges the bitmaps into the images and ends it
  # before it starts a new one. The guest skips stopping its services, which the minimal image does not complete,
  # and the agent answers until the new QEMU process runs.
  pid="$(lxc query /1.0/instances/v1/state | jq --exit-status '.pid')"
  lxc exec v1 -- systemctl reboot --force || true
  for _ in $(seq 60); do
    [ "$(lxc query /1.0/instances/v1/state | jq --exit-status '.pid')" != "${pid}" ] && break
    sleep 1
  done

  [ "$(lxc query /1.0/instances/v1/state | jq --exit-status '.pid')" != "${pid}" ]
  waitInstanceReady v1
  [ "$(_bitmaps v1)" = "$(printf 's3,root,YES\ns4,cbt-blk,YES\ns4,root,YES\ns5,cbt-blk,YES\ns5,root,YES')" ]

  # A guest that powers itself off pauses the QEMU process the same way.
  lxc exec v1 -- systemctl poweroff --force || true
  _wait_stopped v1
  lxc start v1
  waitInstanceReady v1
  [ "$(_bitmaps v1)" = "$(printf 's3,root,YES\ns4,cbt-blk,YES\ns4,root,YES\ns5,cbt-blk,YES\ns5,root,YES')" ]

  sub_test "A guest poweroff after an LXD restart keeps the bitmaps"
  # The daemon is killed so that the instance keeps running, and the new daemon handles the shutdown event of the guest.
  kill -9 "$(< "${LXD_DIR}/lxd.pid")"
  respawn_lxd "${LXD_DIR}" true
  waitInstanceReady v1
  [ "$(_bitmaps v1)" = "$(printf 's3,root,YES\ns4,cbt-blk,YES\ns4,root,YES\ns5,cbt-blk,YES\ns5,root,YES')" ]
  lxc exec v1 -- systemctl poweroff --force || true
  _wait_stopped v1
  lxc start v1
  waitInstanceReady v1
  [ "$(_bitmaps v1)" = "$(printf 's3,root,YES\ns4,cbt-blk,YES\ns4,root,YES\ns5,cbt-blk,YES\ns5,root,YES')" ]

  sub_test "A guest poweroff while LXD is not running keeps the bitmaps"
  # QEMU pauses on the guest poweroff instead of exiting, and the process stays paused until a daemon merges its
  # bitmaps into the images and ends it. The new daemon does so when it starts, and then starts the instance again
  # because it was running when the old daemon was killed.
  # The transient timer of systemd-run is queued behind the boot jobs of the guest, which wait for the network to be
  # online, so the guest must have finished booting for the timer to be activated in time. The wrapper of lxc times
  # out earlier than that wait can take, so the binary is called directly.
  pid="$(lxc query /1.0/instances/v1/state | jq --exit-status '.pid')"
  timeout --foreground 200 "${_LXC}" exec v1 -- systemctl is-system-running --wait > /dev/null || true
  lxc exec v1 -- systemd-run --quiet --no-block --on-active=5 systemctl poweroff --force
  kill -9 "$(< "${LXD_DIR}/lxd.pid")"
  for _ in $(seq 60); do
    [ "$(_qemu_run_state)" = "shutdown" ] && break
    sleep 1
  done

  respawn_lxd "${LXD_DIR}" true
  for _ in $(seq 120); do
    new_pid="$(lxc query /1.0/instances/v1/state | jq --exit-status '.pid' || echo 0)"
    [ "${new_pid}" -gt 0 ] && [ "${new_pid}" != "${pid}" ] && break
    sleep 1
  done

  [ "${new_pid}" -gt 0 ] && [ "${new_pid}" != "${pid}" ]
  ! kill -0 "${pid}" 2>/dev/null || false
  waitInstanceReady v1
  [ "$(_bitmaps v1)" = "$(printf 's3,root,YES\ns4,cbt-blk,YES\ns4,root,YES\ns5,cbt-blk,YES\ns5,root,YES')" ]

  sub_test "A stopped instance commits an overlay file before its volumes are read"
  # An overlay file left by a failed commit contains guest writes that the volume lacks. The guest never writes to
  # the custom block volume on its own, so the pattern is what it must read back, and the commit merges the overlay
  # bitmap into the bitmaps of the volume, so they mark the committed block and nothing else.
  pattern="$(_write_overlay "${blk_uuid}" "${blk_size}")"
  lxc stop -f v1
  [ ! -e "${metadata_images}/${blk_uuid}.overlay.qcow2" ]
  [ "$(_bitmaps v1)" = "$(printf 's3,root,YES\ns4,cbt-blk,YES\ns4,root,YES\ns5,cbt-blk,YES\ns5,root,YES')" ]
  lxc start v1
  waitInstanceReady v1
  [ "$(lxc exec v1 -- head -c 65536 "${blk_dev}" | sha256sum | cut -d' ' -f1)" = "${pattern}" ]
  [ "$(_bitmaps v1)" = "$(printf 's3,root,YES\ns4,cbt-blk,YES\ns4,root,YES\ns5,cbt-blk,YES\ns5,root,YES')" ]
  lxc snapshot v1 s5b --bitmap --disk-volumes all-exclusive
  _nbd_serve nbd v1/s5b --previous-snapshot-uuid "$(_snapshot_uuid v1/s5)"
  nbdinfo --json --map=qemu:dirty-bitmap:s5 "${NBD_URI}/cbt-blk" | jq --exit-status '.[0].offset == 0 and .[0].length == 65536 and .[0].type == 1 and all(.[1:][]; .type == 0)'
  wait "${NBD_PID}"
  lxc delete v1/s5b
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
  [ "$(_bitmaps v1/s6 || echo fail)" = "" ]
  [ "$(_bitmaps v1/s5)" = "$(printf 's3,root,NO\ns4,cbt-blk,NO\ns4,root,NO')" ]

  sub_test "An LXD restart during the snapshot window keeps the bitmaps"
  # A snapshot with a bitmap removes the store nodes until the overlays are committed, so a daemon that dies in
  # between leaves the instance running without them, which is emulated over the monitor socket. The disk bitmaps
  # record the writes of the guest meanwhile, and the new daemon adds the store nodes back when it commits the
  # overlays of the running instances, so the bitmaps are listed again and the next persist merges the whole run. The
  # write of the guest is scheduled before the daemon is killed, as the poweroff above.
  timeout --foreground 200 "${_LXC}" exec v1 -- systemctl is-system-running --wait > /dev/null || true
  lxc exec v1 -- systemd-run --quiet --no-block --on-active=5 dd if=/dev/urandom of="${blk_dev}" bs=64K count=1 seek=1 conv=fsync
  kill -9 "$(< "${LXD_DIR}/lxd.pid")"
  _qemu_monitor '{"execute":"blockdev-del","arguments":{"node-name":"lxdimage_root"}}' '{"execute":"blockdev-del","arguments":{"node-name":"lxdimage_cbt--blk"}}' | jq --exit-status --slurp 'all(.error == null)'
  for _ in $(seq 60); do
    _qemu_monitor '{"execute":"query-named-block-nodes"}' | jq --exit-status '.return[]? | select(."node-name" == "lxd_cbt--blk") | ."dirty-bitmaps"[] | select(.name == "s6") | .count > 0' > /dev/null && break
    sleep 1
  done

  respawn_lxd "${LXD_DIR}" true
  for _ in $(seq 60); do
    [ "$(_bitmaps v1 || true)" = "$(printf 's6,cbt-blk,YES\ns6,root,YES')" ] && break
    sleep 1
  done

  [ "$(_bitmaps v1)" = "$(printf 's6,cbt-blk,YES\ns6,root,YES')" ]
  lxc stop -f v1
  lxc start v1
  waitInstanceReady v1
  lxc snapshot v1 s6b --bitmap --disk-volumes all-exclusive
  _nbd_serve nbd v1/s6b --previous-snapshot-uuid "$(_snapshot_uuid v1/s6)"
  nbdinfo --json --map=qemu:dirty-bitmap:s6 "${NBD_URI}/cbt-blk" | jq --exit-status '[.[] | select(.type == 1) | [.offset, .length]] == [[65536, 65536]]'
  wait "${NBD_PID}"
  lxc delete v1/s6b
  [ "$(_bitmaps v1)" = "$(printf 's6,cbt-blk,YES\ns6,root,YES')" ]

  sub_test "An overlay without a valid overlay bitmap invalidates the bitmaps of the volume"
  # Without the overlay bitmap the writes the overlay holds are unknown, so the commit of the overlay file removes
  # every bitmap of the volume instead of merging into them, as after a crash while the store node was closed for a
  # snapshot with a bitmap. The block is zeroed first, so that the pattern read back is the one the commit wrote.
  lxc exec v1 -- dd if=/dev/zero of="${blk_dev}" bs=64K count=1 conv=fsync
  pattern="$(_write_overlay "${blk_uuid}" "${blk_size}")"
  qemu-img bitmap --remove -f qcow2 "${metadata_images}/${blk_uuid}.overlay.qcow2" writes
  lxc stop -f v1
  [ ! -e "${metadata_images}/${blk_uuid}.overlay.qcow2" ]
  [ "$(_bitmaps v1)" = "s6,root,YES" ]
  lxc start v1
  waitInstanceReady v1
  [ "$(lxc exec v1 -- head -c 65536 "${blk_dev}" | sha256sum | cut -d' ' -f1)" = "${pattern}" ]
  [ "$(_bitmaps v1)" = "s6,root,YES" ]

  sub_test "Restoring a snapshot deletes the bitmaps of the volumes and keeps the copies of the snapshots"
  # The latest snapshot is restored, as ZFS restores no other without deleting the snapshots after it.
  lxc exec v1 -- sh -c 'echo restore > /root/cbt3.bin && sync'
  lxc stop -f v1
  lxc restore v1 s6
  lxc start v1
  waitInstanceReady v1
  [ "$(_bitmaps v1 || echo fail)" = "" ]
  [ "$(_bitmaps v1/s6 || echo fail)" = "" ]
  [ "$(_bitmaps v1/s5)" = "$(printf 's3,root,NO\ns4,cbt-blk,NO\ns4,root,NO')" ]
  ! lxc exec v1 -- test -e /root/cbt3.bin || false

  sub_test "A snapshot that is exported cannot be deleted or renamed"
  _nbd_serve nbd v1/s5
  _nbd_hold
  [ "$(! "${_LXC}" delete v1/s5 2>&1 1>/dev/null)" = 'Error: Failed deleting instance snapshot "v1/s5" in project "default": Snapshot "v1/s5" is exported over NBD: In use' ]
  [ "$(! "${_LXC}" move v1/s5 v1/s5-renamed 2>&1 1>/dev/null)" = 'Error: Snapshot "v1/s5" is exported over NBD: In use' ]
  [ "$(! "${_LXC}" storage volume delete "${pool}" "cbt-blk/${snap_b}" 2>&1 1>/dev/null)" = "Error: Snapshot \"cbt-blk/${snap_b}\" is exported over NBD: In use" ]
  _nbd_release

  sub_test "Deleting a custom volume snapshot removes the bitmap created with it from that volume"
  lxc snapshot v1 s7 --bitmap --disk-volumes all-exclusive
  lxc snapshot v1 s8 --bitmap --disk-volumes all-exclusive
  [ "$(_bitmaps v1)" = "$(printf 's7,cbt-blk,YES\ns7,root,YES\ns8,cbt-blk,YES\ns8,root,YES')" ]
  [ "$(_bitmaps v1/s8)" = "$(printf 's7,cbt-blk,NO\ns7,root,NO')" ]
  lxc storage volume delete "${pool}" "cbt-blk/$(_volume_snapshot_of s7)"
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
    lxc storage volume delete "${pool}" "cbt-blk/${snap_b}" && break
    sleep 1
  done
  _nbd_serve nbd v1/s5
  nbdinfo --list --json "${NBD_URI}" | jq --exit-status '(.exports | length) == 1 and .exports[0]."export-name" == "root"'
  wait "${NBD_PID}"
  [ "$(_bitmaps v1/s5)" = "$(printf 's3,root,NO\ns4,root,NO')" ]
  lxc delete v1/s5 v1/s6 v1/s7 v1/s8 v1/s9
  lxc storage volume delete "${pool}" "cbt-blk/${snap_a}"
  lxc storage volume delete "${pool}" cbt-blk/s8-renamed
  [ "$(_bitmaps v1 || echo fail)" = "" ]

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

  sub_test "Every attached custom volume has its own bitmaps"
  # A second block volume, attached while the instance runs, gets its volume metadata image with its disk.
  lxc storage volume create "${pool}" cbt-data size=32MiB --type block
  lxc storage volume attach "${pool}" cbt-data v1
  data_uuid="$(lxc storage volume get "${pool}" cbt-data volatile.uuid)"
  data_dev="/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_cbt--data"
  for _ in $(seq 30); do
    lxc exec v1 -- test -e "${data_dev}" && break
    sleep 1
  done

  [ -e "${metadata_images}/${data_uuid}.qcow2" ]
  lxc snapshot v1 m1 --bitmap --disk-volumes all-exclusive
  [ "$(_bitmaps v1)" = "$(printf 'm1,cbt-blk,YES\nm1,cbt-data,YES\nm1,root,YES\ns10,cbt-blk,YES\ns10,root,YES')" ]

  # A guest write to one volume marks the bitmap of that volume only.
  lxc exec v1 -- dd if=/dev/urandom of="${data_dev}" bs=64K count=1 seek=2 conv=fsync status=none
  lxc snapshot v1 m2 --bitmap --disk-volumes all-exclusive
  [ "$(_bitmaps v1/m2)" = "$(printf 'm1,cbt-blk,NO\nm1,cbt-data,NO\nm1,root,NO\ns10,cbt-blk,NO\ns10,root,NO')" ]
  _nbd_serve nbd v1/m2 --previous-snapshot-uuid "$(_snapshot_uuid v1/m1)"
  nbdinfo --list --json "${NBD_URI}" | jq --exit-status '[.exports[] | {name: ."export-name", contexts}] | sort_by(.name) == [
    {"name": "cbt-blk", "contexts": ["base:allocation", "qemu:dirty-bitmap:m1"]},
    {"name": "cbt-data", "contexts": ["base:allocation", "qemu:dirty-bitmap:m1"]},
    {"name": "root", "contexts": ["base:allocation", "qemu:dirty-bitmap:m1"]}]'
  wait "${NBD_PID}"
  _nbd_serve nbd v1/m2 --previous-snapshot-uuid "$(_snapshot_uuid v1/m1)"
  nbdinfo --json --map=qemu:dirty-bitmap:m1 "${NBD_URI}/cbt-data" | jq --exit-status '[.[] | select(.type == 1) | [.offset, .length]] == [[131072, 65536]]'
  wait "${NBD_PID}"
  _nbd_serve nbd v1/m2 --previous-snapshot-uuid "$(_snapshot_uuid v1/m1)"
  nbdinfo --json --map=qemu:dirty-bitmap:m1 "${NBD_URI}/cbt-blk" | jq --exit-status 'all(.[]; .type == 0)'
  wait "${NBD_PID}"

  # The devices filter and the previous snapshot UUID apply together.
  _nbd_serve nbd v1/m2 --devices cbt-data --previous-snapshot-uuid "$(_snapshot_uuid v1/m1)"
  nbdinfo --list --json "${NBD_URI}" | jq --exit-status '(.exports | length) == 1 and .exports[0]."export-name" == "cbt-data" and .exports[0].contexts == ["base:allocation", "qemu:dirty-bitmap:m1"]'
  wait "${NBD_PID}"
  _nbd_serve nbd v1/m2 --devices root,cbt-data --previous-snapshot-uuid "$(_snapshot_uuid v1/m2)"
  nbdinfo --list --json "${NBD_URI}" | jq --exit-status '[.exports[] | {name: ."export-name", contexts}] | sort_by(.name) == [
    {"name": "cbt-data", "contexts": ["base:allocation"]},
    {"name": "root", "contexts": ["base:allocation"]}]'
  wait "${NBD_PID}"

  # A volume detached from the running instance loses its bitmaps and its volume metadata image.
  lxc delete v1/m2 v1/m1
  lxc storage volume detach "${pool}" cbt-data v1
  [ ! -e "${metadata_images}/${data_uuid}.qcow2" ]
  lxc storage volume delete "${pool}" cbt-data
  [ "$(_bitmaps v1)" = "$(printf 's10,cbt-blk,YES\ns10,root,YES')" ]

  sub_test "Containers and stopped virtual machines get no bitmaps"
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

  sub_test "A shared volume has no bitmap and no export, and is left out of the snapshot while another instance uses it"
  # Attached to v1 alone, a shared volume is snapshotted with the instance, without a bitmap or an image.
  lxc storage volume create "${pool}" cbt-shared size=32MiB --type block security.shared=true
  lxc storage volume attach "${pool}" cbt-shared v1
  shared_uuid="$(lxc storage volume get "${pool}" cbt-shared volatile.uuid)"
  lxc snapshot v1 s11 --bitmap --disk-volumes all-exclusive
  [ "$(_bitmaps v1)" = "$(printf 's10,cbt-blk,YES\ns10,root,YES\ns11,cbt-blk,YES\ns11,root,YES')" ]
  [ ! -e "${metadata_images}/${shared_uuid}.qcow2" ]
  lxc config show v1/s11 | yq -r --exit-status '.config."volatile.attached_volumes"' | jq --exit-status 'has("cbt-blk") and has("cbt-shared")'
  [ "$(lxc query "/1.0/storage-pools/${pool}/volumes/custom/cbt-shared/snapshots" | jq --exit-status 'length')" = "1" ]
  _nbd_serve nbd v1/s11
  nbdinfo --list --json "${NBD_URI}" | jq --exit-status '[.exports[]."export-name"] | sort == ["cbt-blk", "root"]'
  wait "${NBD_PID}"
  _nbd_serve nbd v1/s11 --devices cbt-shared
  ! nbdinfo "${NBD_URI}" || false
  ! wait "${NBD_PID}" || false
  [[ "$(cat "${NBD_STDERR}")" == 'Error: Snapshot has no volume snapshot with bitmaps for device "cbt-shared"'* ]]

  # Attached to another instance as well, the volume is not snapshotted.
  lxc init --empty v2 --vm
  lxc storage volume attach "${pool}" cbt-shared v2
  lxc snapshot v1 s11b --bitmap --disk-volumes all-exclusive
  lxc config show v1/s11b | yq -r --exit-status '.config."volatile.attached_volumes"' | jq --exit-status 'has("cbt-blk") and (has("cbt-shared") | not)'
  [ "$(lxc query "/1.0/storage-pools/${pool}/volumes/custom/cbt-shared/snapshots" | jq --exit-status 'length')" = "1" ]
  _nbd_serve nbd v1/s11b
  nbdinfo --list --json "${NBD_URI}" | jq --exit-status '[.exports[]."export-name"] | sort == ["cbt-blk", "root"]'
  wait "${NBD_PID}"
  lxc delete v1/s11b
  lxc delete v2
  lxc storage volume detach "${pool}" cbt-shared v1
  lxc storage volume delete "${pool}" cbt-shared

  sub_test "Enabling security.shared deletes the bitmaps of the volume"
  lxc storage volume set "${pool}" cbt-blk security.shared=true
  [ "$(_bitmaps v1)" = "$(printf 's10,root,YES\ns11,root,YES')" ]

  # The copies in an earlier snapshot recorded the writes while the volume was exclusive, and are kept.
  [ "$(_bitmaps v1/s11)" = "$(printf 's10,cbt-blk,NO\ns10,root,NO')" ]
  [ "$(_nbd_contexts cbt-blk nbd v1/s11)" = '["base:allocation","qemu:dirty-bitmap:s10"]' ]
  lxc storage volume unset "${pool}" cbt-blk security.shared
  lxc snapshot v1 s12 --bitmap --disk-volumes all-exclusive
  [ "$(_bitmaps v1)" = "$(printf 's10,root,YES\ns11,root,YES\ns12,cbt-blk,YES\ns12,root,YES')" ]

  # On a stopped instance the volume metadata image of the volume is deleted with its bitmaps, so the next start
  # creates it again without them. The config volume of a stopped instance is not mounted on every backend, so the
  # listing after the start is what shows it.
  lxc stop -f v1
  lxc storage volume set "${pool}" cbt-blk security.shared=true
  [ "$(_bitmaps v1)" = "$(printf 's10,root,YES\ns11,root,YES\ns12,root,YES')" ]
  lxc storage volume unset "${pool}" cbt-blk security.shared
  lxc start v1
  waitInstanceReady v1
  [ -e "${metadata_images}/${blk_uuid}.qcow2" ]
  [ "$(_bitmaps v1)" = "$(printf 's10,root,YES\ns11,root,YES\ns12,root,YES')" ]
  lxc delete v1/s12
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
  wait "${NBD_PID}"
  _nbd_wait_imports
  _nbd_serve storage volume nbd "${pool}" cbt-blk --writable
  nbdcopy --connections=1 "${import_src}" "${NBD_URI}"
  wait "${NBD_PID}"
  _nbd_wait_imports
  [ "$(_bitmaps v1)" = "$(printf 's10,root,YES\ns11,root,YES\ns12,root,YES')" ]

  # The instance cannot start while a session writes one of its volumes.
  _nbd_serve storage volume nbd "${pool}" virtual-machine/v1 --writable
  _nbd_hold
  ! lxc start v1 || false
  _nbd_release
  _nbd_wait_imports

  _nbd_serve storage volume nbd "${pool}" virtual-machine/v1 --writable
  nbdcopy --connections=1 "${s1_copy}" "${NBD_URI}"
  wait "${NBD_PID}"
  _nbd_wait_imports
  [ "$(_bitmaps v1 || echo fail)" = "" ]

  lxc start v1
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

  # A block volume that is not shared is attached to one instance only, so it is detached for the copies.
  lxc storage volume detach "${pool}" cbt-blk v1

  # The metadata images of v1 record neither the writes to v2 nor its snapshots, so v2 starts with new images.
  lxc copy v1 v2
  lxc start v2
  waitInstanceReady v2
  [ "$(_bitmaps v2 || echo fail)" = "" ]
  prepare_vm_for_hard_stop v2
  lxc delete -f v2

  # The backup of the stopped v1 contains its metadata images. The lxc wrapper kills its command after 120s, which
  # the export of a root disk can exceed, so the binary is called directly.
  "${_LXC}" export v1 "${TEST_DIR}/v1.tar.gz" --instance-only
  "${_LXC}" import "${TEST_DIR}/v1.tar.gz" v3
  rm -f "${TEST_DIR}/v1.tar.gz"
  lxc start v3
  waitInstanceReady v3
  [ "$(_bitmaps v3 || echo fail)" = "" ]
  prepare_vm_for_hard_stop v3
  lxc delete -f v3

  # The copy and the export kept the metadata images of v1.
  lxc storage volume attach "${pool}" cbt-blk v1
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
  [ "$(_bitmaps v1 || echo fail)" = "" ]
  lxc start v1
  waitInstanceReady v1

  # The images were created again at the new sizes, without preallocated tables.
  [ "$(lxc exec v1 -- blockdev --getsize64 "${blk_dev}")" = "$((64 * 1024 * 1024))" ]
  _metadata_image_info "${metadata_images}/${root_uuid}.qcow2" | jq --exit-status '.raw == false and .size < 1048576 and .bitmaps == []'
  lxc snapshot v1 s15 --bitmap --disk-volumes all-exclusive
  [ "$(_bitmaps v1)" = "$(printf 's15,cbt-blk,YES\ns15,root,YES')" ]
  lxc stop -f v1
  [ "$(_bitmaps v1)" = "$(printf 's15,cbt-blk,YES\ns15,root,YES')" ]
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
