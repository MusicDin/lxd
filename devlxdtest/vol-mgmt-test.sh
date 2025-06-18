#!/bin/bash

set -exo

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
        if [[ "${vol}" == *"/"* ]]; then
            echo "Skipping snapshot ${vol} removal"
        else
            lxc storage volume delete "${pool}" "${vol}"
        fi
    done
else
    lxc storage create "${pool}" "${poolDriver}"
fi

# > Tests

for instType in container vm; do
    echo "==> TEST: Test devLXD volume management through a ${instType}"

    inst="${instPrefix}-${instType}"

    if [ "${instType}" = "vm" ]; then
        ARGS="--vm"
    fi

    lxc launch ubuntu-minimal:24.04 "${inst}" ${ARGS}
    waitInstance "${inst}"

    echo "===> TEST: Test devLXD conectivity and access"

    lxc exec "${inst}" -- curl -f --unix-socket /dev/lxd/sock -X GET lxd/1.0

    # Test unauthorized access.
    ! lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools?recursion=1 || false

    # Enable volume management.
    lxc config set "${inst}" security.devlxd.management.volumes=true

    echo "===> PASS: Test devLXD conectivity and access"
    echo "===> TEST: Test storage pools"

    # List storage pools (fail - only recurison is supported).
    ! lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools || false

    # List storage pools - Check that our storage pool is retrieved.
    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools?recursion=1 | jq -e --arg pool "$pool" '.[] | select(.name == $pool)'

    # Get storage pool.
    ! lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools/invalid-pool || false
    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools/${pool} | jq

    echo "===> PASS: Test storage pools"
    echo "===> TEST: Test storage volumes"

    # Get storage volumes (fail - non-custom volume requested).
    ! lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools/${pool}/volumes/image || false
    ! lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools/${pool}/volumes/container || false
    ! lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools/${pool}/volumes/virtual-machine || false

    # Get storage volumes (ok - custom volumes requested).
    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools/${pool}/volumes/custom | jq -e 'length == 0'

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
    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools/${pool}/volumes/custom | jq -e 'length == 2'
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

    # Update storage volume
    lxc exec "${inst}" -- curl --unix-socket /dev/lxd/sock \
        -X PATCH lxd/1.0/storage-pools/${pool}/volumes/custom/vol-01 \
        -H "Content-Type: application/json" \
        -d '{"config": {"size": "25MiB"}}'
    lxc exec "${inst}" -- curl -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools/${pool}/volumes/custom/vol-01 | jq | tee >(jq -e '.config.size == "25MiB" and .description == "Updated volume"')

    # Update storage volume (fail - incorrect ETag).
    ! lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock \
        -X PUT lxd/1.0/storage-pools/${pool}/volumes/custom/vol-01 \
        -H "Content-Type: application/json" \
        -H "If-Match: incorrect-etag" \
        -d '{"description": "This must fail"}' || false

    echo "===> PASS: Test storage volumes"
    echo "===> TEST: Test instance devices"

    # List devices.
    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/instances/${inst}

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

    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X PUT lxd/1.0/instances/${inst} -H "Content-Type: application/json" -d "${attachReq}"
    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/instances/${inst} | jq -e -r '.devices."vol-01".source == "vol-01"'
    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/instances/${inst}| jq -e -r '.devices."vol-01".source == "vol-01"'

    # Detach new device.
    detachReq=$(cat <<EOF
{
    "devices": {}
}
EOF
)

    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X PUT lxd/1.0/instances/${inst} -H "Content-Type: application/json" -d "${detachReq}"
    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/instances/${inst} | jq '.devices' | jq 'length == 0'

    echo "===> PASS: Test instance devices"
    echo "===> TEST: Test storage volume snapshots"

    # Create storage volume snapshots.
    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/storage-pools/${pool}/volumes/custom/vol-02/snapshots?recursion=1" | jq -e 'length == 0'

    opID=$(lxc exec "${inst}" -- curl -f --unix-socket /dev/lxd/sock -X POST "lxd/1.0/storage-pools/${pool}/volumes/custom/vol-02/snapshots" \
        -H "Content-Type: application/json" \
        -d '{"name": "snap-01"}' | jq -er .id)
    lxc exec "${inst}" -- curl -f --unix-socket /dev/lxd/sock -X GET "lxd/1.0/operations/${opID}/wait?timeout=10"

    opID=$(lxc exec "${inst}" -- curl -f --unix-socket /dev/lxd/sock -X POST "lxd/1.0/storage-pools/${pool}/volumes/custom/vol-02/snapshots" \
        -H "Content-Type: application/json" \
        -d '{"name": "snap-02"}' | jq -er .id)
    lxc exec "${inst}" -- curl -f --unix-socket /dev/lxd/sock -X GET "lxd/1.0/operations/${opID}/wait?timeout=10"

    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/storage-pools/${pool}/volumes/custom/vol-02/snapshots?recursion=1"
    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/storage-pools/${pool}/volumes/custom/vol-02/snapshots?recursion=1" | jq -e 'length == 2'

    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/storage-pools/${pool}/volumes/custom/vol-02/snapshots/snap-01" | jq -e '.name == "snap-01" and .content_type == "filesystem"'

    # Delete storage volume snapshot.
    opID=$(lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X DELETE "lxd/1.0/storage-pools/${pool}/volumes/custom/vol-02/snapshots/snap-01" | jq -er .id)
    lxc exec "${inst}" -- curl -f --unix-socket /dev/lxd/sock -X GET "lxd/1.0/operations/${opID}/wait?timeout=10"
    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/storage-pools/${pool}/volumes/custom/vol-02/snapshots?recursion=1" | jq -e 'length == 1'
    opID=$(lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X DELETE "lxd/1.0/storage-pools/${pool}/volumes/custom/vol-02/snapshots/snap-02" | jq -er .id)
    lxc exec "${inst}" -- curl -f --unix-socket /dev/lxd/sock -X GET "lxd/1.0/operations/${opID}/wait?timeout=10"
    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/storage-pools/${pool}/volumes/custom/vol-02/snapshots?recursion=1" | jq -e 'length == 0'
    ! lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET "lxd/1.0/storage-pools/${pool}/volumes/custom/vol-02/snapshots/snap-02" || false

    echo "===> PASS: Test storage volume snapshots"

    # Delete storage volumes.
    ! lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X DELETE lxd/1.0/storage-pools/${pool}/volumes/custom/non-existing-volume || false
    lxc exec "${inst}" -- curl -f --unix-socket /dev/lxd/sock -X DELETE lxd/1.0/storage-pools/${pool}/volumes/custom/vol-01
    lxc exec "${inst}" -- curl -f --unix-socket /dev/lxd/sock -X DELETE lxd/1.0/storage-pools/${pool}/volumes/custom/vol-02

    # Ensure storage volumes are deleted.
    lxc exec "${inst}" -- curl -f -s --unix-socket /dev/lxd/sock -X GET lxd/1.0/storage-pools/${pool}/volumes/custom | jq 'length == 0'

    # Cleanup.
    lxc delete "${inst}" --force

    echo "==> PASS: Test devLXD volume management through a ${instType}"
done

# Cleanup.
lxc storage delete "${pool}"

echo ""
echo "PASS: Volume management tests have passed"
