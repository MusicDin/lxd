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

# _bitmaps prints the bitmaps of an instance or of an instance snapshot as "name,device,recording" lines, sorted.
_bitmaps() {
  lxc bitmap list "$@" --format csv -c ndr | sort
}

# _bitmap_uuid prints the UUID of the named bitmap of an instance or of an instance snapshot.
_bitmap_uuid() {
  lxc bitmap show "$1" "$2" | yq --exit-status '.uuid'
}

# _wait_stopped waits for the instance to reach the STOPPED state.
_wait_stopped() {
  for _ in $(seq 60); do
    [ "$(lxc list -f csv -c s "$1")" = "STOPPED" ] && return 0
    sleep 1
  done

  return 1
}

test_storage_block_tracking_vm() {
  if ! check_dependencies nbdinfo nbdcopy qemu-img qemu-io qemu-nbd qemu-storage-daemon; then
    export TEST_UNMET_REQUIREMENT="Missing nbdinfo, nbdcopy, qemu-img, qemu-io, qemu-nbd or qemu-storage-daemon"
    return
  fi

  local pool orig_volume_size root_dev root_size s1_uuid s2_uuid s1_copy s2_copy reconstructed extents offset length chunk blk_dev blk_size blk_checksum blk_copy operation_uuid pid metadata_images root_uuid blk_uuid overlay pattern import_src
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

  sub_test "A snapshot with a bitmap creates the bitmap on the root disk"
  lxc snapshot v1 s1 --bitmap
  [ "$(_bitmaps v1)" = "s1,root,YES" ]
  lxc bitmap show v1 s1 | yq --exit-status '.name == "s1" and (.uuid | length) == 36 and (.volumes | length) == 1 and .volumes[0].device == "root" and .volumes[0].type == "virtual-machine" and .volumes[0].name == "v1" and .volumes[0].recording == true and .volumes[0].granularity > 0'
  [ "$(lxc bitmap show v1 s1 | yq --exit-status '.volumes[0].uuid')" = "${root_uuid}" ]
  [ "$(lxc bitmap show v1 s1 | yq --exit-status '.volumes[0].pool')" = "${pool}" ]
  s1_uuid="$(_bitmap_uuid v1 s1)"
  [ "$(! "${_LXC}" bitmap show v1 missing 2>&1 1>/dev/null)" = "Error: Bitmap not found" ]

  # The snapshot metadata image is removed from the config volume once the config volume snapshot includes it.
  [ -d "${metadata_images}" ]
  [ "$(find "${metadata_images}" -name "${root_uuid}.*.qcow2" | wc -l)" = "0" ]

  # The bitmap of a snapshot cannot be created twice under one name while the bitmap is live.
  lxc delete v1/s1
  [ "$(! "${_LXC}" snapshot v1 s1 --bitmap 2>&1 1>/dev/null)" = 'Error: Bitmap "s1" already exists on disk "root"' ]
  [ "$(_bitmaps v1)" = "s1,root,YES" ]

  # A snapshot that was created before any bitmap existed has no bitmaps and cannot be exported.
  lxc snapshot v1 s0
  [ "$(_bitmaps v1/s0)" = "" ]
  _nbd_serve nbd v1/s0
  ! nbdinfo "${NBD_URI}" || false
  ! wait "${NBD_PID}" || false
  [[ "$(cat "${NBD_STDERR}")" == "Error: Snapshot was not created with a bitmap"* ]]
  lxc delete v1/s0

  # Dirty a few MiB of the root disk.
  lxc exec v1 -- sh -c 'dd if=/dev/urandom of=/root/cbt.bin bs=1M count=4 && sync'

  sub_test "The next snapshot gets a copy of the bitmap"
  lxc snapshot v1 s2 --bitmap
  s2_uuid="$(_bitmap_uuid v1 s2)"
  [ "${s2_uuid}" != "${s1_uuid}" ]
  [ "$(_bitmaps v1)" = "$(printf 's1,root,YES\ns2,root,YES')" ]
  [ "$(_bitmaps v1/s2)" = "s1,root,NO" ]
  [ "$(_bitmap_uuid v1/s2 s1)" = "${s1_uuid}" ]
  lxc bitmap show v1/s2 s1 | yq --exit-status '.volumes[0].device == "root" and .volumes[0].type == "virtual-machine" and .volumes[0].name == "v1" and .volumes[0].recording == false'
  [ "$(! "${_LXC}" bitmap show v1/s2 s2 2>&1 1>/dev/null)" = "Error: Bitmap not found" ]
  [ "$(! "${_LXC}" bitmap show v1/missing s1 2>&1 1>/dev/null)" = 'Error: Instance snapshot not found' ]
  [ "$(! "${_LXC}" bitmap delete v1/s2 s1 2>&1 1>/dev/null)" = "Error: Bitmaps cannot be deleted from a snapshot" ]

  sub_test "The snapshot export publishes the copies of the bitmaps"
  root_dev="/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_root"
  root_size="$(lxc exec v1 -- blockdev --getsize64 "${root_dev}")"

  # The export is read-only, has the size of the disk and publishes the copy of the bitmap under both of its
  # names, but not the bitmap created with the snapshot.
  _nbd_serve nbd v1/s2
  nbdinfo --list --json "${NBD_URI}" | jq --exit-status --argjson size "${root_size}" --arg uuid "${s1_uuid}" '[.exports[] | select(."export-name" == "root")][0] | .is_read_only and (."export-size" == $size) and (.contexts | index("qemu:dirty-bitmap:s1") != null) and (.contexts | index("qemu:dirty-bitmap:" + $uuid + "/s1") != null) and (.contexts | index("qemu:dirty-bitmap:s2") == null)'
  wait "${NBD_PID}"

  # The first snapshot with a bitmap has no copy of an earlier bitmap, so it serves data only.
  _nbd_serve nbd v1/s1
  nbdinfo --json "${NBD_URI}/root" | jq --exit-status '.exports[0] | .is_read_only and (.contexts == ["base:allocation"])'
  wait "${NBD_PID}"

  # An unknown device is refused.
  _nbd_serve nbd v1/s2 --devices missing
  ! nbdinfo "${NBD_URI}/root" || false
  ! wait "${NBD_PID}" || false
  [[ "$(cat "${NBD_STDERR}")" == 'Error: Snapshot has no volume snapshot with bitmaps for device "missing"'* ]]

  sub_test "An incremental backup reconstructs the second snapshot from the first"
  # The export relays a single client, so nbdcopy must not open parallel connections.
  s1_copy="$(mktemp -p "${TEST_DIR}" s1_copy.XXX)"
  _nbd_serve nbd v1/s1
  nbdcopy --connections=1 "${NBD_URI}/root" "${s1_copy}"
  wait "${NBD_PID}"

  s2_copy="$(mktemp -p "${TEST_DIR}" s2_copy.XXX)"
  _nbd_serve nbd v1/s2
  nbdcopy --connections=1 "${NBD_URI}/root" "${s2_copy}"
  wait "${NBD_PID}"

  [ "$(stat -c %s "${s1_copy}")" = "${root_size}" ]
  [ "$(stat -c %s "${s2_copy}")" = "${root_size}" ]
  ! cmp -s "${s1_copy}" "${s2_copy}" || false

  # The dirty extents (type 1) of the copy of the bitmap are the blocks written between s1 and s2.
  _nbd_serve nbd v1/s2
  extents="$(nbdinfo --map="qemu:dirty-bitmap:${s1_uuid}/s1" --json "${NBD_URI}/root" | jq -r '.[] | select(.type == 1) | "\(.offset) \(.length)"')"
  wait "${NBD_PID}"
  [ -n "${extents}" ]

  # Patch each dirty extent read from the s2 export into the full copy of s1.
  reconstructed="$(mktemp -p "${TEST_DIR}" reconstructed.XXX)"
  cp "${s1_copy}" "${reconstructed}"
  chunk="$(mktemp -p "${TEST_DIR}" chunk.XXX)"
  while read -r offset length; do
    [ "$((offset % 65536))" = 0 ]
    [ "$((length % 65536))" = 0 ]
    _nbd_serve nbd v1/s2
    qemu-img dd -f raw -O raw bs=65536 skip="$((offset / 65536))" count="$((length / 65536))" "if=${NBD_URI}/root" "of=${chunk}"
    wait "${NBD_PID}"
    dd if="${chunk}" of="${reconstructed}" bs=65536 seek="$((offset / 65536))" conv=notrunc status=none
  done <<< "${extents}"
  cmp "${reconstructed}" "${s2_copy}"
  rm -f "${chunk}" "${reconstructed}"

  sub_test "Every client opens its own session"
  # A raw connection keeps one session open while a second client reads the same snapshot.
  _nbd_serve nbd v1/s2
  _nbd_hold
  local first_holder_pid first_nbd_pid
  first_holder_pid="${NBD_HOLDER_PID}"
  first_nbd_pid="${NBD_PID}"
  _nbd_serve nbd v1/s2
  nbdinfo --json "${NBD_URI}/root" | jq --exit-status --argjson size "${root_size}" '.exports[0] | .is_read_only and (."export-size" == $size)'
  wait "${NBD_PID}"
  NBD_HOLDER_PID="${first_holder_pid}"
  NBD_PID="${first_nbd_pid}"
  _nbd_release

  sub_test "NBD export is listed as an operation and cancelling it ends the export"
  _nbd_serve nbd v1/s2
  _nbd_hold
  operation_uuid=""
  for _ in $(seq 20); do
    operation_uuid="$(lxc operation list --format csv | grep -F "TASK,Exporting instance snapshot over NBD,RUNNING" | cut -d, -f1 || true)"
    [ -n "${operation_uuid}" ] && break
    sleep 0.5
  done
  [ -n "${operation_uuid}" ]
  lxc operation show "${operation_uuid}" | yq --exit-status '.metadata.entity_url == "/1.0/instances/v1/snapshots/s2"'

  # Cancelling the operation closes the connection, so the client and the export command exit on their own.
  lxc operation delete "${operation_uuid}"
  _nbd_release
  for _ in $(seq 10); do
    lxc operation show "${operation_uuid}" | yq --exit-status '.status == "Cancelled"' > /dev/null && break
    sleep 0.5
  done
  lxc operation show "${operation_uuid}" | yq --exit-status '.status == "Cancelled"'

  sub_test "An instance snapshot with a bitmap covers the attached block volume"
  # The custom block volume is a raw attached device the guest never writes to on its own, so its content is
  # stable and can be checksummed against the export.
  blk_dev="/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_cbt--blk"
  lxc exec v1 -- sync
  blk_size="$(lxc exec v1 -- blockdev --getsize64 "${blk_dev}")"
  blk_checksum="$(lxc exec v1 -- sha256sum "${blk_dev}" | cut -d' ' -f1)"

  lxc snapshot v1 s3 --bitmap --disk-volumes all-exclusive
  [ "$(_bitmaps v1)" = "$(printf 's1,root,YES\ns2,root,YES\ns3,cbt-blk,YES\ns3,root,YES')" ]
  [ "$(_bitmaps v1/s3)" = "$(printf 's1,root,NO\ns2,root,NO')" ]
  [ "$(lxc bitmap show v1 s3 | yq --exit-status '.volumes | length')" = "2" ]
  [ "$(lxc bitmap show v1 s3 | yq --exit-status '.volumes[0].uuid')" = "${blk_uuid}" ]
  lxc bitmap show v1 s3 | yq --exit-status '.volumes[0].device == "cbt-blk" and .volumes[0].type == "custom" and .volumes[0].name == "cbt-blk"'
  lxc storage volume snapshot list "${pool}" cbt-blk --format csv -c n | grep -xF "snap0"

  # Every volume of the snapshot is listed under an export named after its disk device, with its own size and
  # its copies of the bitmaps.
  _nbd_serve nbd v1/s3
  nbdinfo --list --json "${NBD_URI}" | jq --exit-status --argjson root "${root_size}" --argjson blk "${blk_size}" '
    ([.exports[] | select(."export-name" == "root")][0] | (."export-size" == $root) and (.contexts | index("qemu:dirty-bitmap:s2") != null))
    and ([.exports[] | select(."export-name" == "cbt-blk")][0] | (."export-size" == $blk) and (.contexts == ["base:allocation"]))'
  wait "${NBD_PID}"

  # Selecting the custom disk export yields the guest's own view of the volume.
  blk_copy="$(mktemp -p "${TEST_DIR}" blk_copy.XXX)"
  _nbd_serve nbd v1/s3
  nbdcopy --connections=1 "${NBD_URI}/cbt-blk" "${blk_copy}"
  wait "${NBD_PID}"
  [ "$(stat -c %s "${blk_copy}")" = "${blk_size}" ]
  [ "$(sha256sum "${blk_copy}" | cut -d' ' -f1)" = "${blk_checksum}" ]
  rm -f "${blk_copy}"

  # The devices filter serves a subset of the volumes.
  _nbd_serve nbd v1/s3 --devices cbt-blk
  nbdinfo --list --json "${NBD_URI}" | jq --exit-status '(.exports | length) == 1 and .exports[0]."export-name" == "cbt-blk"'
  wait "${NBD_PID}"

  sub_test "The next snapshot copies the bitmaps of both volumes"
  lxc snapshot v1 s4 --bitmap --disk-volumes all-exclusive
  [ "$(_bitmaps v1/s4)" = "$(printf 's1,root,NO\ns2,root,NO\ns3,cbt-blk,NO\ns3,root,NO')" ]
  _nbd_serve nbd v1/s4
  nbdinfo --list --json "${NBD_URI}" | jq --exit-status '[.exports[] | select(."export-name" == "cbt-blk")][0].contexts | index("qemu:dirty-bitmap:s3") != null'
  wait "${NBD_PID}"

  sub_test "Bitmaps are kept across a stop, a start and a reboot"
  # The minimal image runs no logind, so a graceful stop via the ACPI power button never completes.
  lxc stop -f v1
  [ -e "${metadata_images}/${root_uuid}.qcow2" ]
  [ -e "${metadata_images}/${blk_uuid}.qcow2" ]
  [ "$(_bitmaps v1)" = "$(printf 's1,root,YES\ns2,root,YES\ns3,cbt-blk,YES\ns3,root,YES\ns4,cbt-blk,YES\ns4,root,YES')" ]
  [ "$(_bitmap_uuid v1 s1)" = "${s1_uuid}" ]
  lxc start v1
  waitInstanceReady v1
  [ "$(_bitmaps v1)" = "$(printf 's1,root,YES\ns2,root,YES\ns3,cbt-blk,YES\ns3,root,YES\ns4,cbt-blk,YES\ns4,root,YES')" ]
  [ "$(_bitmap_uuid v1 s1)" = "${s1_uuid}" ]
  [ ! -e "${metadata_images}/${root_uuid}.qcow2" ]

  # A reboot from inside the guest pauses QEMU rather than ending it, so the bitmaps are persisted and reloaded.
  lxc exec v1 -- systemctl reboot || true
  sleep 5
  waitInstanceReady v1
  [ "$(_bitmaps v1)" = "$(printf 's1,root,YES\ns2,root,YES\ns3,cbt-blk,YES\ns3,root,YES\ns4,cbt-blk,YES\ns4,root,YES')" ]

  # A guest that powers itself off ends the same way as a graceful stop, QEMU pauses and LXD persists the bitmaps
  # before it quits.
  lxc exec v1 -- systemctl poweroff || true
  _wait_stopped v1
  lxc start v1
  waitInstanceReady v1
  [ "$(_bitmaps v1)" = "$(printf 's1,root,YES\ns2,root,YES\ns3,cbt-blk,YES\ns3,root,YES\ns4,cbt-blk,YES\ns4,root,YES')" ]

  sub_test "A guest poweroff after an LXD restart keeps the bitmaps"
  # The daemon is killed so that the instance keeps running, and the new daemon handles the shutdown event of the guest.
  kill -9 "$(< "${LXD_DIR}/lxd.pid")"
  respawn_lxd "${LXD_DIR}" true
  [ "$(_bitmaps v1)" = "$(printf 's1,root,YES\ns2,root,YES\ns3,cbt-blk,YES\ns3,root,YES\ns4,cbt-blk,YES\ns4,root,YES')" ]
  lxc exec v1 -- systemctl poweroff || true
  _wait_stopped v1
  lxc start v1
  waitInstanceReady v1
  [ "$(_bitmaps v1)" = "$(printf 's1,root,YES\ns2,root,YES\ns3,cbt-blk,YES\ns3,root,YES\ns4,cbt-blk,YES\ns4,root,YES')" ]

  sub_test "A crash deletes the bitmaps of the volumes and the next start commits an overlay"
  # An overlay left by a crash during a snapshot with a bitmap contains the guest's latest writes.
  # The guest never writes to the custom block volume on its own, so the pattern is what it must read back.
  overlay="${metadata_images}/${blk_uuid}.overlay.qcow2"
  qemu-img create -f qcow2 "${overlay}" "${blk_size}"
  qemu-io -f qcow2 -c "write -P 0xab 0 64k" "${overlay}"
  pattern="$(head -c 65536 /dev/zero | tr '\0' '\253' | sha256sum | cut -d' ' -f1)"

  pid="$(lxc query /1.0/instances/v1/state | jq --exit-status '.pid')"
  kill -9 "${pid}"
  _wait_stopped v1
  lxc start v1
  waitInstanceReady v1
  [ "$(_bitmaps v1)" = "" ]
  [ "$(_bitmaps v1/s4)" = "$(printf 's1,root,NO\ns2,root,NO\ns3,cbt-blk,NO\ns3,root,NO')" ]
  [ ! -e "${overlay}" ]
  [ "$(lxc exec v1 -- head -c 65536 "${blk_dev}" | sha256sum | cut -d' ' -f1)" = "${pattern}" ]

  sub_test "Restoring a snapshot deletes the bitmaps of the volumes and keeps the copies of the snapshots"
  lxc snapshot v1 s5 --bitmap --disk-volumes all-exclusive
  [ "$(_bitmaps v1)" = "$(printf 's5,cbt-blk,YES\ns5,root,YES')" ]
  lxc stop -f v1
  lxc restore v1 s2
  lxc start v1
  waitInstanceReady v1
  [ "$(_bitmaps v1)" = "" ]
  [ "$(_bitmaps v1/s5)" = "" ]
  [ "$(_bitmaps v1/s4)" = "$(printf 's1,root,NO\ns2,root,NO\ns3,cbt-blk,NO\ns3,root,NO')" ]
  ! lxc exec v1 -- test -e /root/cbt.bin || false

  sub_test "A snapshot that is exported cannot be deleted or renamed"
  _nbd_serve nbd v1/s4
  _nbd_hold
  [ "$(! "${_LXC}" delete v1/s4 2>&1 1>/dev/null)" = 'Error: Snapshot "v1/s4" is exported over NBD: In use' ]
  [ "$(! "${_LXC}" move v1/s4 v1/s4-renamed 2>&1 1>/dev/null)" = 'Error: Snapshot "v1/s4" is exported over NBD: In use' ]
  [ "$(! "${_LXC}" storage volume snapshot delete "${pool}" cbt-blk snap1 2>&1 1>/dev/null)" = 'Error: Snapshot "cbt-blk/snap1" is exported over NBD: In use' ]
  _nbd_release

  sub_test "Deleting a custom volume snapshot leaves the instance snapshot export without that volume"
  # The export is released after its relay ends, so the first delete attempt can race it.
  for _ in $(seq 10); do
    lxc storage volume snapshot delete "${pool}" cbt-blk snap1 && break
    sleep 1
  done
  _nbd_serve nbd v1/s4
  nbdinfo --list --json "${NBD_URI}" | jq --exit-status '(.exports | length) == 1 and .exports[0]."export-name" == "root"'
  wait "${NBD_PID}"

  sub_test "Deleting a bitmap removes it from every volume and keeps the copies of the snapshots"
  lxc snapshot v1 s6 --bitmap --disk-volumes all-exclusive
  lxc snapshot v1 s7 --bitmap --disk-volumes all-exclusive
  [ "$(_bitmaps v1)" = "$(printf 's6,cbt-blk,YES\ns6,root,YES\ns7,cbt-blk,YES\ns7,root,YES')" ]
  lxc bitmap delete v1 s6
  [ "$(_bitmaps v1)" = "$(printf 's7,cbt-blk,YES\ns7,root,YES')" ]
  [ "$(_bitmaps v1/s7)" = "$(printf 's6,cbt-blk,NO\ns6,root,NO')" ]
  [ "$(! "${_LXC}" bitmap delete v1 s6 2>&1 1>/dev/null)" = "Error: Bitmap not found" ]

  # A stopped instance deletes the bitmap from the metadata images of its volumes.
  lxc stop -f v1
  lxc bitmap delete v1 s7
  [ "$(_bitmaps v1)" = "" ]
  [ "$(! "${_LXC}" bitmap delete v1 s7 2>&1 1>/dev/null)" = "Error: Bitmap not found" ]
  lxc start v1
  waitInstanceReady v1
  [ "$(_bitmaps v1)" = "" ]

  sub_test "Renaming an instance snapshot deletes the bitmaps of the volumes"
  lxc snapshot v1 s8 --bitmap
  [ "$(_bitmaps v1)" = "s8,root,YES" ]
  lxc move v1/s7 v1/s7-renamed
  [ "$(_bitmaps v1)" = "" ]
  [ "$(_bitmaps v1/s7-renamed)" = "$(printf 's6,cbt-blk,NO\ns6,root,NO')" ]
  _nbd_serve nbd v1/s7-renamed
  nbdinfo --json "${NBD_URI}/root" | jq --exit-status '.exports[0].contexts | index("qemu:dirty-bitmap:s6") != null'
  wait "${NBD_PID}"

  sub_test "Containers, stopped virtual machines and shared volumes get no bitmaps"
  ensure_import_testimage
  lxc launch testimage c1
  [ "$(! "${_LXC}" snapshot c1 --bitmap 2>&1 1>/dev/null)" = "Error: A snapshot with a bitmap requires a running virtual machine" ]
  lxc delete -f c1

  lxc stop -f v1
  [ "$(! "${_LXC}" snapshot v1 --bitmap 2>&1 1>/dev/null)" = "Error: A snapshot with a bitmap requires a running virtual machine" ]
  lxc start v1
  waitInstanceReady v1

  lxc storage volume create "${pool}" cbt-shared size=32MiB --type block security.shared=true
  lxc storage volume attach "${pool}" cbt-shared v1
  lxc snapshot v1 s9 --bitmap --disk-volumes all-exclusive
  [ "$(_bitmaps v1)" = "$(printf 's9,cbt-blk,YES\ns9,root,YES')" ]
  lxc storage volume detach "${pool}" cbt-shared v1
  lxc storage volume delete "${pool}" cbt-shared

  sub_test "Enabling security.shared deletes the bitmaps of the volume"
  lxc storage volume set "${pool}" cbt-blk security.shared=true
  [ "$(_bitmaps v1)" = "s9,root,YES" ]
  lxc storage volume unset "${pool}" cbt-blk security.shared
  lxc snapshot v1 s10 --bitmap --disk-volumes all-exclusive
  [ "$(_bitmaps v1)" = "$(printf 's10,cbt-blk,YES\ns10,root,YES\ns9,root,YES')" ]

  sub_test "A read-write NBD export of a stopped volume deletes its bitmaps"
  lxc stop -f v1
  [ "$(_bitmaps v1)" = "$(printf 's10,cbt-blk,YES\ns10,root,YES\ns9,root,YES')" ]

  # A read-only export of a stopped volume keeps its bitmaps.
  _nbd_serve storage volume nbd "${pool}" cbt-blk
  nbdinfo --json "${NBD_URI}" | jq --exit-status --argjson size "${blk_size}" '.exports[0] | .is_read_only and (."export-size" == $size)'
  wait "${NBD_PID}"
  [ "$(_bitmaps v1)" = "$(printf 's10,cbt-blk,YES\ns10,root,YES\ns9,root,YES')" ]

  import_src="$(mktemp -p "${TEST_DIR}" import_src.XXX)"
  head -c "${blk_size}" /dev/urandom > "${import_src}"
  _nbd_serve storage volume nbd "${pool}" cbt-blk --writable
  nbdcopy --connections=1 "${import_src}" "${NBD_URI}"
  wait "${NBD_PID}"
  [ "$(_bitmaps v1)" = "$(printf 's10,root,YES\ns9,root,YES')" ]

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
  lxc snapshot v1 s11 --bitmap
  [ "$(_bitmaps v1)" = "s11,root,YES" ]
  lxc stop -f v1

  # The metadata images of v1 record neither the writes to v2 nor its snapshots.
  lxc copy v1 v2
  lxc start v2
  waitInstanceReady v2
  [ ! -e "${LXD_DIR}/virtual-machines/v2/metadata_images" ]
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
  [ ! -e "${LXD_DIR}/virtual-machines/v3/metadata_images" ]
  [ "$(_bitmaps v3)" = "" ]
  prepare_vm_for_hard_stop v3
  lxc delete -f v3

  # The copy and the export kept the metadata images of v1.
  lxc start v1
  waitInstanceReady v1
  [ "$(_bitmaps v1)" = "s11,root,YES" ]

  sub_test "A resize and a detach delete the bitmaps of the volume"
  lxc snapshot v1 s12 --bitmap --disk-volumes all-exclusive
  [ "$(_bitmaps v1)" = "$(printf 's11,root,YES\ns12,cbt-blk,YES\ns12,root,YES')" ]
  lxc stop -f v1
  lxc storage volume set "${pool}" cbt-blk size=64MiB
  [ "$(_bitmaps v1)" = "$(printf 's11,root,YES\ns12,root,YES')" ]
  lxc config device set v1 root size=5GiB
  [ "$(_bitmaps v1)" = "" ]
  lxc start v1
  waitInstanceReady v1
  lxc snapshot v1 s13 --bitmap --disk-volumes all-exclusive
  [ "$(_bitmaps v1)" = "$(printf 's13,cbt-blk,YES\ns13,root,YES')" ]
  lxc stop -f v1
  [ -e "${metadata_images}/${blk_uuid}.qcow2" ]
  lxc storage volume detach "${pool}" cbt-blk v1
  [ ! -e "${metadata_images}/${blk_uuid}.qcow2" ]
  [ "$(_bitmaps v1)" = "s13,root,YES" ]

  # Cleanup.
  rm -f "${s1_copy}" "${s2_copy}" "${TEST_DIR}"/nbd_stderr.*
  lxc delete -f v1
  lxc storage volume delete "${pool}" cbt-blk

  if [ -n "${orig_volume_size:-}" ]; then
    # Restore the volume.size.
    lxc storage set "${pool}" volume.size "${orig_volume_size}"
  fi
}
