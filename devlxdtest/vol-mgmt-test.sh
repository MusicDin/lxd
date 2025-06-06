#!/bin/bash

set -ex

poolDriver="zfs"
instPrefix="vol-mgmt-test"
pool="vol-mgmt-test"
project="vol-mgmt-test"
authGroup="${instPrefix}-group"
authIdentity="devlxd/${instPrefix}-identity"

# waitInstance waits for the VM to become ready.
waitInstance() {
        local instance="$1"
        local timeout="${2:-60}"

        if [ -z "${instance}" ]; then
                echo "waitInstance: missing argument: instance name"
                return 1
        fi

        echo "Waiting instance ${instance} to become ready ..."
        for j in $(seq 1 "${timeout}"); do
                local procCount=$(lxc info "${instance}" --project "${project}" | awk '/Processes:/ {print $2}')
                if [ -n "${procCount}" ] && [ ! "${procCount}" = "-1" ]; then
                        echo "Instance ${instance} ready after ${j} seconds."
                        break
                fi

                if [ "$j" -eq "${timeout}" ]; then
                        echo "Instance ${instance} still not ready after ${timeout} seconds!"
                        return 1
                fi

                sleep 1
        done
}


# > Setup
echo "==> Cleanup of potentially existing resources ..."

# Remove existing devLXD identities.
lxc auth identity delete "${authIdentity}" || true
lxc auth group delete "${instPrefix}-group" || true

# Remove existing instances.
for instType in container vm; do
    inst="${instPrefix}-${instType}"

    if lxc list "${inst}" --project "${project}" --format csv --columns n | grep -qE "^${inst}$"; then
        lxc delete "${inst}" --project "${project}" --force
    fi
done

# Cleanup existing volumes before the test.
if lxc storage list --format csv | awk -F, '{ print $1}' | grep -qE "^${pool}$"; then
    for vol in $(lxc storage volume ls "${pool}" --project "${project}" --format csv --columns n); do
        if lxc image show "${vol}" --project "${project}" &> /dev/null; then
            lxc image delete "${vol}" --project "${project}"
        else
            lxc storage volume delete "${pool}" "${vol}" --project "${project}"
        fi
    done
else
    lxc storage create "${pool}" "${poolDriver}"
fi

if [ "${project}" != "default" ]; then
    lxc project delete "${project}" || true
    lxc project create "${project}" --config features.images=false
fi

# > Tests
echo "==> Test devLXD volume management ..."

for instType in container vm; do
    inst="${instPrefix}-${instType}"

    if [ "${instType}" = "vm" ]; then
        ARGS="--vm"
    fi

    lxc launch ubuntu-minimal:24.04 "${inst}" --project "${project}" --storage "${pool}" ${ARGS}
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

    # > Storage pools.

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

echo ""
echo "PASS: Volume management tests have passed"
