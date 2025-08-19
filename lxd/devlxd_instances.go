package main

import (
	"maps"
	"net/http"

	"github.com/canonical/lxd/lxd/device/filters"
	"github.com/canonical/lxd/lxd/instance"
	"github.com/canonical/lxd/shared/api"
)

type devLXDDeviceAccessValidator func(device map[string]string) bool

// patchInstanceDevices updates an existing instance (api.Instance) with devices from the
// request instance (api.DevLXDInstance), and adjusts the device ownership configuration
// accordingly.
//
// Device actions are determined as follows:
// - Add:
//   - Condition: New device that devLXD can manage and is not present in the existing devices.
//   - Action:    Adds new device to the instance and set its owner in the instance config.
//
// - Update:
//   - Condition: Existing device that devLXD can manage and is present in the new devices.
//   - Action:    Updates existing device in the instance.
//
// - Remove:
//   - Condition: Existing device that devLXD can manage is set to "null" in the request.
//   - Action:    Removes existing device from the instance and removes its owner from the instance config.
func patchInstanceDevices(inst *api.Instance, req api.DevLXDInstance, serviceAccountID string, isDeviceAccessible devLXDDeviceAccessValidator) error {
	newDevices := make(map[string]map[string]string)

	// Pass local devices, as non-local devices cannot be owned.
	ownedDevices, otherDevices := getOwnedDevices(*inst, serviceAccountID)

	// Merge new devices into existing ones.
	for name, device := range req.Devices {
		_, isOwned := ownedDevices[name]

		if device == nil {
			// Device is being removed. Check if the device is owned.
			// For consistency with LXD API, we do not error out if
			// the device is not found.
			if !isOwned {
				continue
			}

			// Ensure devLXD has sufficient permissions to manage the device.
			// Pass old device to the validator, as new device is nil.
			if isDeviceAccessible != nil && !isDeviceAccessible(device) {
				return api.StatusErrorf(http.StatusForbidden, "Not authorized to delete device %q", name)
			}

			// Device is removed, so remove the owner config key.
			delete(inst.Config, "volatile."+name+".devlxd.owner")
		} else {
			// Device is being added or updated.
			// Ensure devLXD has sufficient permissions to manage the device.
			if isDeviceAccessible != nil && !isDeviceAccessible(device) {
				return api.StatusErrorf(http.StatusForbidden, "Not authorized to manage device %q", name)
			}

			// In case of an existing device, ensure the device is accessible to devLXD.
			_, exists := inst.ExpandedDevices[name]
			if exists && !isOwned {
				return api.StatusErrorf(http.StatusForbidden, "Not authorized to update device %q", name)
			}

			// At this point we know that the ownership is correct (there is
			// no existing unowned device), so we can safely set the device owner
			// in instance configuration.
			// inst.Config["volatile."+name+".devlxd.owner"] = serviceAccountID # TODO: Uncomment once available.
		}

		newDevices[name] = device
	}

	// Retain owned devices that are not present in the request.
	for name, device := range ownedDevices {
		_, ok := newDevices[name]
		if !ok {
			newDevices[name] = device
		}
	}

	// Retain non-owned devices.
	maps.Copy(newDevices, otherDevices)

	inst.Devices = newDevices
	return nil
}

// getOwnedDevices extracts instance devices that are owned by the provided service account.
// Two maps are returned, one containg owned devices, and the other containing the remaining
// devices.
func getOwnedDevices(inst api.Instance, serviceAccountID string) (ownedDevices map[string]map[string]string, otherDevices map[string]map[string]string) {
	ownedDevices = make(map[string]map[string]string)
	otherDevices = make(map[string]map[string]string)

	for name, device := range inst.Devices {
		if inst.Config["volatile."+name+".devlxd.owner"] == serviceAccountID {
			ownedDevices[name] = device
		} else {
			otherDevices[name] = device
		}
	}

	return ownedDevices, otherDevices
}

// newDevLXDDeviceAccessValidator returns a device validator function that
// checks if the given device is accessible by the devLXD.
//
// For example, disk device is accessible if the appropriate security flag
// is enabled on the instance and the device represents a custom volume.
func newDevLXDDeviceAccessValidator(inst instance.Instance) devLXDDeviceAccessValidator {
	diskDeviceAllowed := hasInstanceSecurityFeatures(inst, devLXDSecurityMgmtVolumesKey)

	return func(device map[string]string) bool {
		return filters.IsCustomVolumeDisk(device) && diskDeviceAllowed
	}
}
