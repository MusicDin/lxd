test_devlxd() {
  ensure_import_testimage
  fingerprint="$(lxc image info testimage | awk '/^Fingerprint:/ {print $2}')"

  # Ensure testimage is not set as cached.
  lxd sql global "UPDATE images SET cached=0 WHERE fingerprint=\"${fingerprint}\""

  (
    cd devlxd-client || return
    # Use -buildvcs=false here to prevent git complaining about untrusted directory when tests are run as root.
    CGO_ENABLED=0 go build -tags netgo -v -buildvcs=false ./...
  )

  lxc launch testimage devlxd -c security.devlxd=false

  ! lxc exec devlxd -- test -S /dev/lxd/sock || false
  lxc config unset devlxd security.devlxd
  lxc exec devlxd -- test -S /dev/lxd/sock
  lxc file push --mode 0755 "devlxd-client/devlxd-client" devlxd/bin/

  ### Test bearer token authentication

  # Check that auth is untrusted by default
  lxc exec devlxd -- devlxd-client get-state | jq -e '.auth == "untrusted"'

  # Create a bearer identity and issue a token
  lxc auth identity create devlxd/foo
  devlxd_token1="$(lxc auth identity token issue devlxd/foo --quiet)"

  # Check that the token is valid (devlxd can be called with the token and auth is trusted).
  lxc exec --env DEVLXD_BEARER_TOKEN="${devlxd_token1}" devlxd -- devlxd-client get-state | jq -e '.auth == "trusted"'

  # Issue another token, the old token should be invalid (so devlxd calls fail) and the new one valid.
  devlxd_token2="$(lxc auth identity token issue devlxd/foo --quiet)"
  [ "$(! lxc exec --env DEVLXD_BEARER_TOKEN="${devlxd_token1}" devlxd -- sh -c 'devlxd-client get-state')" = 'Failed to verify bearer token: Token is not valid: token signature is invalid: signature is invalid' ]
  lxc exec --env DEVLXD_BEARER_TOKEN="${devlxd_token2}" devlxd -- devlxd-client get-state | jq -e '.auth == "trusted"'

  # Revoke the token, it should no longer be valid.
  subject="$(lxc query /1.0/auth/identities/bearer/foo | jq -r .id)"
  lxc auth identity token revoke devlxd/foo
  [ "$(! lxc exec --env DEVLXD_BEARER_TOKEN="${devlxd_token2}" devlxd -- sh -c 'devlxd-client get-state')" = "Failed to verify bearer token: Identity \"${subject}\" (bearer) not found" ]

  # Issue a new token, it should be valid
  devlxd_token3="$(lxc auth identity token issue devlxd/foo --quiet)"
  lxc exec --env DEVLXD_BEARER_TOKEN="${devlxd_token3}" devlxd -- devlxd-client get-state | jq -e '.auth == "trusted"'

  # Delete the identity, the token should no longer be valid.
  lxc auth identity delete devlxd/foo
  [ "$(! lxc exec --env DEVLXD_BEARER_TOKEN="${devlxd_token3}" devlxd -- sh -c 'devlxd-client get-state')" = "Failed to verify bearer token: Identity \"${subject}\" (bearer) not found" ]

  # Create a token with an expiry
  lxc auth identity create devlxd/foo
  devlxd_token4="$(lxc auth identity token issue devlxd/foo --quiet --expiry 1S)"

  # It's initially valid
  lxc exec --env DEVLXD_BEARER_TOKEN="${devlxd_token4}" devlxd -- devlxd-client get-state | jq -e '.auth == "trusted"'

  # It's not valid after the expiry
  sleep 1
  [ "$(! lxc exec --env DEVLXD_BEARER_TOKEN="${devlxd_token4}" devlxd -- sh -c 'devlxd-client get-state')" = 'Failed to verify bearer token: Token is not valid: token has invalid claims: token is expired' ]

  # Clean up
  lxc auth identity delete devlxd/foo

  # No secret remains in the database after the identity was deleted
  [ "$(lxd sql global --format csv 'SELECT COUNT(*) FROM secrets WHERE entity_id = (SELECT id FROM identities WHERE name = "foo")')" = 0 ]

  # Try to get a host's private image from devlxd.
  [ "$(lxc exec devlxd -- devlxd-client image-export "${fingerprint}")" = "Forbidden" ]
  lxc config set devlxd security.devlxd.images true
  # Trying to get a private image should return a not found error so that the client can't infer the existence
  # of an image with the provided fingerprint.
  [ "$(lxc exec devlxd -- devlxd-client image-export "${fingerprint}")" = "Not Found" ]
  lxd sql global "UPDATE images SET cached=1 WHERE fingerprint=\"${fingerprint}\""
  # No output means the export succeeded.
  [ -z "$(lxc exec devlxd -- devlxd-client image-export "${fingerprint}")" ]

  lxc config set devlxd user.foo bar
  [ "$(lxc exec devlxd -- devlxd-client user.foo)" = "bar" ]

  lxc config set devlxd user.foo "bar %s bar"
  [ "$(lxc exec devlxd -- devlxd-client user.foo)" = "bar %s bar" ]

  # Make sure instance configuration keys are not accessible
  [ "$(lxc exec devlxd -- devlxd-client security.nesting)" = "Forbidden" ]
  lxc config set devlxd security.nesting true
  [ "$(lxc exec devlxd -- devlxd-client security.nesting)" = "Forbidden" ]

  cmd=$(unset -f lxc; command -v lxc)
  ${cmd} exec devlxd -- devlxd-client monitor-websocket > "${TEST_DIR}/devlxd-websocket.log" &
  client_websocket=$!

  ${cmd} exec devlxd -- devlxd-client monitor-stream > "${TEST_DIR}/devlxd-stream.log" &
  client_stream=$!

  (
    cat << EOF
{
  "type": "config",
  "timestamp": "0001-01-01T00:00:00Z",
  "metadata": {
    "key": "user.foo",
    "old_value": "bar",
    "value": "baz"
  }
}
{
  "type": "device",
  "timestamp": "0001-01-01T00:00:00Z",
  "metadata": {
    "action": "added",
    "config": {
      "path": "/mnt",
      "source": "${TEST_DIR}",
      "type": "disk"
    },
    "name": "mnt"
  }
}
{
  "type": "device",
  "timestamp": "0001-01-01T00:00:00Z",
  "metadata": {
    "action": "removed",
    "config": {
      "path": "/mnt",
      "source": "${TEST_DIR}",
      "type": "disk"
    },
    "name": "mnt"
  }
}
EOF
  ) > "${TEST_DIR}/devlxd.expected"

  MATCH=0

  for _ in $(seq 10); do
    lxc config set devlxd user.foo bar
    lxc config set devlxd security.nesting true

    true > "${TEST_DIR}/devlxd-websocket.log"
    true > "${TEST_DIR}/devlxd-stream.log"

    lxc config set devlxd user.foo baz
    lxc config set devlxd security.nesting false
    lxc config device add devlxd mnt disk source="${TEST_DIR}" path=/mnt
    lxc config device remove devlxd mnt

    if [ "$(tr -d '\0' < "${TEST_DIR}/devlxd-websocket.log" | md5sum | cut -d' ' -f1)" != "$(md5sum "${TEST_DIR}/devlxd.expected" | cut -d' ' -f1)" ] || [ "$(tr -d '\0' < "${TEST_DIR}/devlxd-stream.log" | md5sum | cut -d' ' -f1)" != "$(md5sum "${TEST_DIR}/devlxd.expected" | cut -d' ' -f1)" ]; then
      sleep 0.5
      continue
    fi

    MATCH=1
    break
  done

  kill -9 "${client_websocket}"
  kill -9 "${client_stream}"

  lxc monitor --type=lifecycle > "${TEST_DIR}/devlxd.log" &
  monitorDevlxdPID=$!

  # Test instance Ready state
  lxc info devlxd | grep -xF 'Status: RUNNING'
  lxc exec devlxd -- devlxd-client ready-state true
  [ "$(lxc config get devlxd volatile.last_state.ready)" = "true" ]

  [ "$(grep -Fc "instance-ready" "${TEST_DIR}/devlxd.log")" = "1" ]

  lxc info devlxd | grep -xF 'Status: READY'
  lxc exec devlxd -- devlxd-client ready-state false
  [ "$(lxc config get devlxd volatile.last_state.ready)" = "false" ]

  [ "$(grep -Fc "instance-ready" "${TEST_DIR}/devlxd.log")" = "1" ]

  lxc info devlxd | grep -xF 'Status: RUNNING'

  kill -9 "${monitorDevlxdPID}" || true

  shutdown_lxd "${LXD_DIR}"
  respawn_lxd "${LXD_DIR}" true

  # volatile.last_state.ready should be unset during daemon init
  [ -z "$(lxc config get devlxd volatile.last_state.ready)" ]

  lxc monitor --type=lifecycle > "${TEST_DIR}/devlxd.log" &
  monitorDevlxdPID=$!

  lxc exec devlxd -- devlxd-client ready-state true
  [ "$(lxc config get devlxd volatile.last_state.ready)" = "true" ]

  [ "$(grep -Fc "instance-ready" "${TEST_DIR}/devlxd.log")" = "1" ]

  lxc stop -f devlxd
  [ "$(lxc config get devlxd volatile.last_state.ready)" = "false" ]

  lxc start devlxd
  lxc exec devlxd -- devlxd-client ready-state true
  [ "$(lxc config get devlxd volatile.last_state.ready)" = "true" ]

  [ "$(grep -Fc "instance-ready" "${TEST_DIR}/devlxd.log")" = "2" ]

  # Check device configs are available and that NIC hwaddr is available even if volatile.
  hwaddr=$(lxc config get devlxd volatile.eth0.hwaddr)
  [ "$(lxc exec devlxd -- devlxd-client devices | jq -r .eth0.hwaddr)" = "${hwaddr}" ]

  lxc delete devlxd --force
  kill -9 "${monitorDevlxdPID}" || true

  [ "${MATCH}" = "1" ]
}

test_devlxd_volume_management_api() {
  local testName="vol-mgmt-test"
  local project="default"

  local instImage="testimage"
  local instPrefix="${testName}"
  local instTypes="container" # "container vm" - VMs are currently not supported in LXD test suite.
  local pool="${testName}"
  local poolDriver=$(storage_backend "$LXD_DIR")
  local authGroup="${testName}-group"
  local authIdentity="devlxd/${testName}-identity"

  lxc storage create "${pool}" "${poolDriver}"
  if [ "${project}" != "default" ]; then
    lxc project create "${project}" --config features.images=false
  fi

  for instType in $instTypes; do
    inst="${instPrefix}-${instType}"

    if [ "${instType}" = "vm" ]; then
        ARGS="--vm"
    fi

    lxc launch "${instImage}" "${inst}" --project "${project}" --storage "${pool}" ${ARGS}
    waitInstance "${inst}"

    lxc exec "${inst}" --project "${project}" -- curl -f --unix-socket /dev/lxd/sock -X GET lxd/1.0

    # Test devLXD authorization (volume management security flag).
    # Fail when the security flag is not set.
    ! lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/storage-pools/${pool}" || false

    # Succeed when the security flag is set.
    lxc config set "${inst}" --project "${project}" security.devlxd.management.volumes=true
    lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/storage-pools/${pool}"

    # Test devLXD authentication (devLXD identity).
    # Fail when token is not passed.
    ! lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/instances/${inst}" || false

    # Fail when invalid token is passed.
    ! lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/instances/${inst}" -H "Authorization: Bearer invalid-token" || false

    # Fail when a vaild identity token is passed, but the identity does not have permissions.
    lxc auth identity create "${authIdentity}"
    token=$(lxc auth identity token issue "${authIdentity}" --quiet)
    ! lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/instances/${inst}" -H "Authorization: Bearer ${token}" || false

    # Succeed when a valid identity token is passed and the identity has permissions.
    lxc auth group create "${authGroup}"
    lxc auth group permission add "${authGroup}" instance "${inst}" can_view project="${project}"
    lxc auth identity group add "${authIdentity}" "${authGroup}"
    lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/instances/${inst}" -H "Authorization: Bearer ${token}"

    # Get storage pool.
    ! lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/storage-pools/invalid-pool" || false
    lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/storage-pools/${pool}" | jq

    # Get storage volumes (fail - non-custom volume requested).
    ! lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/storage-pools/${pool}/volumes/image" || false
    ! lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/storage-pools/${pool}/volumes/container" || false
    ! lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/storage-pools/${pool}/volumes/virtual-machine" || false

    # Get storage volumes (ok - custom volumes requested).
    lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/storage-pools/${pool}/volumes/custom" | jq 'length == 0'

    # Create storage volumes (fail - invalid or unknown type).
    ! lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X POST "lxd/1.0/storage-pools/${pool}/volumes/custom" -H "Content-Type: application/json" -d '{"name": "vol-type-mismatch", "type": "container"}' || false
    ! lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X POST "lxd/1.0/storage-pools/${pool}/volumes/virtual-machine" -H "Content-Type: application/json" -d '{"name": "vol-unknown-type"}' || false
    ! lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X POST "lxd/1.0/storage-pools/${pool}/volumes/image" -H "Content-Type: application/json" -d '{"name": "vol-type-not-allowed"}' || false

    # Create a custom storage volume.
    lxc exec "${inst}" --project "${project}" -- curl -f --unix-socket /dev/lxd/sock \
        -X POST lxd/1.0/storage-pools/${pool}/volumes/custom \
        -H "Content-Type: application/json" \
        -d '{"name": "vol-01", "config": {"size": "10MiB"}}'

    lxc exec "${inst}" --project "${project}" -- curl -f --unix-socket /dev/lxd/sock \
        -X POST lxd/1.0/storage-pools/${pool}/volumes/custom \
        -H "Content-Type: application/json" \
        -d '{"name": "vol-02", "config": {"size": "10MiB"}, "type": "custom"}'

    # Volume already exists (fail).
    ! lxc exec "${inst}" --project "${project}" -- curl -f --unix-socket /dev/lxd/sock -X POST "lxd/1.0/storage-pools/${pool}/volumes/custom" -H "Content-Type: application/json" -d '{"name": "vol-01"}' || false

    # Verify created storage volumes.
    lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/storage-pools/${pool}/volumes/custom" | jq 'length == 2'
    lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/storage-pools/${pool}/volumes/custom/vol-02"
    lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/storage-pools/${pool}/volumes/custom/vol-01" | jq | tee >(jq -e '.config.size == "10MiB" and .description == ""')

    # Update storage volume (fail - incorrect ETag).
    ! lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock \
        -X PUT "lxd/1.0/storage-pools/${pool}/volumes/custom/vol-01" \
        -H "Content-Type: application/json" \
        -H "If-Match: incorrect-etag" \
        -d '{"description": "This must fail"}' || false

    # Update storage volume (ok - correct ETag).
    etag=$(lxc exec "${inst}" --project "${project}" -- curl -s -f -i --unix-socket /dev/lxd/sock -X GET "lxd/1.0/storage-pools/${pool}/volumes/custom/vol-01" | grep -i etag | awk -F': ' '{print $2}')
    if [ -z "${etag}" ]; then
        echo "Failed to retrieve ETag for storage volume."
        exit 1
    fi

    lxc exec "${inst}" --project "${project}" -- curl --unix-socket /dev/lxd/sock \
        -X PUT "lxd/1.0/storage-pools/${pool}/volumes/custom/vol-01" \
        -H "Content-Type: application/json" \
        -H "If-Match: ${etag}" \
        -d '{"config": {"size": "20MiB"}, "description": "Updated volume"}'
    lxc exec "${inst}" --project "${project}" -- curl -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/storage-pools/${pool}/volumes/custom/vol-01" | jq | tee >(jq -e '.config.size == "20MiB" and .description == "Updated volume"')

    # Update storage volume
    lxc exec "${inst}" --project "${project}" -- curl --unix-socket /dev/lxd/sock \
        -X PUT "lxd/1.0/storage-pools/${pool}/volumes/custom/vol-01" \
        -H "Content-Type: application/json" \
        -d '{"config": {"size": "25MiB"}}'
    lxc exec "${inst}" --project "${project}" -- curl -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/storage-pools/${pool}/volumes/custom/vol-01" | jq | tee >(jq -e '.config.size == "25MiB" and .description == "Updated volume"')

    # Update storage volume (fail - incorrect ETag).
    ! lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock \
        -X PUT "lxd/1.0/storage-pools/${pool}/volumes/custom/vol-01" \
        -H "Content-Type: application/json" \
        -H "If-Match: incorrect-etag" \
        -d '{"description": "This must fail"}' || false

    # List devices.
    lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/instances/${inst}" -H "Authorization: Bearer ${token}"

    # Attach new device.
    attachReq=$(cat <<EOF
{
  "devices": {
    "vol-01": {
      "type": "disk",
      "pool": "${pool}",
      "source": "vol-01",
      "path": "/mnt/vol-01"
    }
  }
}
EOF
)

    # Fail without edit permission. Add edit permission and retry.
    ! lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X PATCH "lxd/1.0/instances/${inst}" -H "Authorization: Bearer ${token}" -H "Content-Type: application/json" -d "${attachReq}" || false
    lxc auth group permission add "${authGroup}" instance "${inst}" can_edit project="${project}"
    lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X PATCH "lxd/1.0/instances/${inst}" -H "Authorization: Bearer ${token}" -H "Content-Type: application/json" -d "${attachReq}" | jq -r .id
    lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/instances/${inst}" -H "Authorization: Bearer ${token}" | jq -e -r '.devices."vol-01".source == "vol-01"'

    # Detach new device.
    detachReq=$(cat <<EOF
{
  "devices": {
    "vol-01": null
  }
}
EOF
)

    lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X PATCH "lxd/1.0/instances/${inst}" -H "Authorization: Bearer ${token}" -H "Content-Type: application/json" -d "${detachReq}"
    lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/instances/${inst}" -H "Authorization: Bearer ${token}" | jq '.devices' | jq 'length == 0'

    # Delete storage volumes.
    ! lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X DELETE "lxd/1.0/storage-pools/${pool}/volumes/custom/non-existing-volume" || false
    lxc exec "${inst}" --project "${project}" -- curl -f --unix-socket /dev/lxd/sock -X DELETE "lxd/1.0/storage-pools/${pool}/volumes/custom/vol-01"
    lxc exec "${inst}" --project "${project}" -- curl -f --unix-socket /dev/lxd/sock -X DELETE "lxd/1.0/storage-pools/${pool}/volumes/custom/vol-02"

    # Ensure storage volumes are deleted.
    lxc exec "${inst}" --project "${project}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/storage-pools/${pool}/volumes/custom" | jq 'length == 0'

    # Cleanup.
    lxc delete "${inst}" --project "${project}" --force
    lxc auth identity delete "${authIdentity}"
    lxc auth group delete "${authGroup}"
  done

  # Cleanup.
  lxc storage delete "${pool}"
  if [ "${project}" != "default" ]; then
      lxc project delete "${project}"
  fi
}
