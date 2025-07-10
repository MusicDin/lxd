package main

import (
	"github.com/canonical/lxd/lxd/device/filters"
	"github.com/canonical/lxd/lxd/instance"
)

type devLXDDeviceAccessValidator func(device map[string]string) bool

// newDevLXDDeviceAccessValidator returns a device validator function that
// checks if the given device is accessible by the devLXD.
func newDevLXDDeviceAccessValidator(inst instance.Instance) devLXDDeviceAccessValidator {
	diskDeviceAllowed := hasInstanceSecurityFeatures(inst, devLXDSecurityMgmtVolumesKey)

	return func(device map[string]string) bool {
		return filters.IsCustomVolumeDisk(device) && diskDeviceAllowed
	}
}
