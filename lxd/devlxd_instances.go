package main

import (
	"github.com/canonical/lxd/lxd/device/filters"
	"github.com/canonical/lxd/lxd/instance"
	"github.com/canonical/lxd/shared/api"
)

type devLXDDeviceAccessValidator func(device map[string]string) bool

// getAccessibleDevices extracts accessible instance devices. Two maps are returned,
// one containg accessible devices, and the other containing the remaining devices.
//
// Device is accessible if:
// - Device owner matches the service account ID.
// - Device type matches the allowed device types.
//
// Additionall restrictions::
// - Disk device is accessible only if it is a custom volume disk device.
func getAccessibleDevices(inst api.Instance, serviceAccountID string, isDeviceAccessible devLXDDeviceAccessValidator) (accessibleDevices map[string]map[string]string, otherDevices map[string]map[string]string) {
	accessibleDevices = make(map[string]map[string]string)
	otherDevices = make(map[string]map[string]string)

	for name, device := range inst.Devices {
		if isDeviceAccessible != nil && isDeviceAccessible(device) && inst.Config["volatile."+name+".devlxd.owner"] == serviceAccountID {
			accessibleDevices[name] = device
		} else {
			otherDevices[name] = device
		}
	}

	return accessibleDevices, otherDevices
}

// newDevLXDDeviceAccessValidator returns a device validator function that
// checks if the given device is accessible by the devLXD.
func newDevLXDDeviceAccessValidator(inst instance.Instance) devLXDDeviceAccessValidator {
	diskDeviceAllowed := hasInstanceSecurityFeatures(inst, devLXDSecurityMgmtVolumesKey)

	return func(device map[string]string) bool {
		return filters.IsCustomVolumeDisk(device) && diskDeviceAllowed
	}
}
