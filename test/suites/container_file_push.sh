#
# Specific edge cases tests tested using test LXD client.
#

test_container_file_push() {
  inst="c-file-push"

  lxc launch testimage "${inst}"

  (
    cd lxd-client || return
    # Use -buildvcs=false here to prevent git complaining about untrusted directory when tests are run as root.
    CGO_ENABLED=0 go build -tags netgo -v -buildvcs=false ./...
  )

  # This ensures strings.Reader works correctly with the content-type check.
  # The specific here is that the net/http package will configure the
  # content-length on the request, which in LXD triggers content-type check.
  lxd-client/lxd-client file-push "${inst}" /tmp/status.txt "success"
  [ "$(lxc exec "${inst}" -- cat /tmp/status.txt)" = "success" ]

  lxc delete "${inst}" --force
}
