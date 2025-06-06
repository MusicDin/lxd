#!/bin/bash

set -ex

poolDriver="zfs"
instPrefix="vol-mgmt-test"
pool="vol-mgmt-test"

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
                local procCount=$(lxc info "${instance}" | awk '/Processes:/ {print $2}')
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

# Remove existing instances.
for instType in container vm; do
    inst="${instPrefix}-${instType}"

    if lxc list "${inst}" --format csv --columns n | grep -qE "^${inst}$"; then
        lxc delete "${inst}" --force
    fi
done

# Cleanup existing volumes before the test.
if lxc storage list --format csv | awk -F, '{ print $1}' | grep -qE "^${pool}$"; then
    for vol in $(lxc storage volume ls "${pool}" --format csv --columns n); do
        lxc storage volume delete "${pool}" "${vol}"
    done
else
    lxc storage create "${pool}" "${poolDriver}"
fi

# > Tests

for instType in container vm; do
    inst="${instPrefix}-${instType}"

    if [ "${instType}" = "vm" ]; then
        ARGS="--vm"
    fi

    lxc launch ubuntu-minimal:24.04 "${inst}" ${ARGS}
    waitInstance "${inst}"

    lxc exec "${inst}" -- curl -f --unix-socket /dev/lxd/sock -X GET lxd/1.0

    # Test unauthorized access.
    ! lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools?recursion=1 || false

    # Enable volume management.
    lxc config set "${inst}" security.devlxd.volume_management=true

    # > Storage pools.

    # List storage pools (fail - only recurison is supported).
    ! lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools || false

    # List storage pools - Check that our storage pool is retrieved.
    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools?recursion=1 | jq -e --arg pool "$pool" '.[] | select(.name == $pool)'

    # Get storage pool.
    ! lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools/invalid-pool || false
    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools/${pool} | jq

    # Get storage volumes (fail - non-custom volume requested).
    ! lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools/${pool}/volumes/image || false
    ! lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools/${pool}/volumes/container || false
    ! lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools/${pool}/volumes/virtual-machine || false

    # Get storage volumes (ok - custom volumes requested).
    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools/${pool}/volumes/custom | jq 'length == 0'

    # Create storage volumes (fail - invalid or unknown type).
    ! lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X POST lxd/1.0/storage-pools/${pool}/volumes/custom -H "Content-Type: application/json" -d '{"name": "vol-type-mismatch", "type": "container"}' || false
    ! lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X POST lxd/1.0/storage-pools/${pool}/volumes/virtual-machine -H "Content-Type: application/json" -d '{"name": "vol-unknown-type"}' || false
    ! lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X POST lxd/1.0/storage-pools/${pool}/volumes/image -H "Content-Type: application/json" -d '{"name": "vol-type-not-allowed"}' || false

    # Create a custom storage volume.
    lxc exec "${inst}" -- curl -f --unix-socket /dev/lxd/sock \
        -X POST lxd/1.0/storage-pools/${pool}/volumes/custom \
        -H "Content-Type: application/json" \
        -d '{"name": "vol-01", "config": {"size": "10MiB"}}'

    lxc exec "${inst}" -- curl -f --unix-socket /dev/lxd/sock \
        -X POST lxd/1.0/storage-pools/${pool}/volumes/custom \
        -H "Content-Type: application/json" \
        -d '{"name": "vol-02", "config": {"size": "10MiB"}, "type": "custom"}'

    # Volume already exists (fail).
    ! lxc exec "${inst}" -- curl -f --unix-socket /dev/lxd/sock -X POST lxd/1.0/storage-pools/${pool}/volumes/custom -H "Content-Type: application/json" -d '{"name": "vol-01"}' || false

    # Verify created storage volumes.
    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools/${pool}/volumes/custom | jq 'length == 2'
    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools/${pool}/volumes/custom/vol-02
    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools/${pool}/volumes/custom/vol-01 | jq | tee >(jq -e '.config.size == "10MiB" and .description == ""')

    # Update storage volume (fail - incorrect ETag).
    ! lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock \
        -X PUT lxd/1.0/storage-pools/${pool}/volumes/custom/vol-01 \
        -H "Content-Type: application/json" \
        -H "If-Match: incorrect-etag" \
        -d '{"description": "This must fail"}' || false

    # Update storage volume (ok - correct ETag).
    etag=$(lxc exec "${inst}" -- curl -s -f -i --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools/${pool}/volumes/custom/vol-01 | grep -i etag | awk -F': ' '{print $2}')
    if [ -z "${etag}" ]; then
        echo "Failed to retrieve ETag for storage volume."
        exit 1
    fi

    lxc exec "${inst}" -- curl --unix-socket /dev/lxd/sock \
        -X PUT lxd/1.0/storage-pools/${pool}/volumes/custom/vol-01 \
        -H "Content-Type: application/json" \
        -H "If-Match: ${etag}" \
        -d '{"config": {"size": "20MiB"}, "description": "Updated volume"}'
    lxc exec "${inst}" -- curl -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools/${pool}/volumes/custom/vol-01 | jq | tee >(jq -e '.config.size == "20MiB" and .description == "Updated volume"')

    # Patch storage volume
    lxc exec "${inst}" -- curl --unix-socket /dev/lxd/sock \
        -X PUT lxd/1.0/storage-pools/${pool}/volumes/custom/vol-01 \
        -H "Content-Type: application/json" \
        -d '{"config": {"size": "25MiB"}}'
    lxc exec "${inst}" -- curl -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools/${pool}/volumes/custom/vol-01 | jq | tee >(jq -e '.config.size == "25MiB" and .description == "Updated volume"')

    # Update storage volume (fail - incorrect ETag).
    ! lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock \
        -X PUT lxd/1.0/storage-pools/${pool}/volumes/custom/vol-01 \
        -H "Content-Type: application/json" \
        -H "If-Match: incorrect-etag" \
        -d '{"description": "This must fail"}' || false

    # List devices.
    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/instances/${inst}/devices

    # Attach new device.
    attachReq=$(cat <<EOF
{
    "type": "disk",
    "pool": "${pool}",
    "source": "vol-01",
    "path": "/mnt/vol-01"
}
EOF
)

    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X POST lxd/1.0/instances/${inst}/devices -H "Content-Type: application/json" -d "${attachReq}"
    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/instances/${inst}/devices | jq -e -r '."vol-01".source == "vol-01"'
    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/instances/${inst}/devices/vol-01 | jq -e -r '.source == "vol-01"'
    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X DELETE lxd/1.0/instances/${inst}/devices/vol-01
    ! lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/instances/${inst}/devices/vol-01 || false

    # Delete storage volumes.
    ! lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X DELETE lxd/1.0/storage-pools/${pool}/volumes/custom/non-existing-volume || false
    lxc exec "${inst}" -- curl -f --unix-socket /dev/lxd/sock -X DELETE lxd/1.0/storage-pools/${pool}/volumes/custom/vol-01
    lxc exec "${inst}" -- curl -f --unix-socket /dev/lxd/sock -X DELETE lxd/1.0/storage-pools/${pool}/volumes/custom/vol-02

    # Ensure storage volumes are deleted.
    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools/${pool}/volumes/custom | jq 'length == 0'

    # Cleanup.
    lxc delete "${inst}" --force
done

# Cleanup.
lxc storage delete "${pool}"

echo ""
echo "PASS: Volume management tests have passed"
