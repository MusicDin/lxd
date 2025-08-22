package main

import (
	"github.com/canonical/lxd/lxd/device/filters"
	"github.com/canonical/lxd/lxd/instance"
	"github.com/canonical/lxd/shared/api"
)

type devLXDDeviceAccessValidator func(device map[string]string) bool

// getOwnedDevices extracts instance devices that are owned by the provided identity.
// Two maps are returned, one containg owned devices, and the other containing the remaining
// devices.
func getOwnedDevices(inst api.Instance, identityID string) (ownedDevices map[string]map[string]string, otherDevices map[string]map[string]string) {
	ownedDevices = make(map[string]map[string]string)
	otherDevices = make(map[string]map[string]string)

	for name, device := range inst.Devices {
		if inst.Config["volatile."+name+".devlxd.owner"] == identityID {
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
