package main

import (
	"net/http"

	"github.com/canonical/lxd/lxd/device/filters"
	"github.com/canonical/lxd/lxd/instance"
	"github.com/canonical/lxd/shared/api"
)

type devLXDDeviceAccessValidator func(device map[string]string) bool

// generateDevLXDInstanceDevices compares the existing LXD instance (api.Instance) against the incoming
// request instance (api.DevLXDInstancePut) and derives the instance devices that need to be patched
// alongside the instance configuration containing the appropriate device ownership changes.
//
// The function also verifies that the device is accessible to DevLXD using the provided validator function.
// In addition, only devices owned by the provided identity ID can be updated or removed.
// New devices can only be added if they are accessible by DevLXD and do not already exist in the instance's
// expanded devices.
//
// Note that the function does not mutate the provided instance.
// Instead, it returns new maps for devices and config to be applied.
func generateDevLXDInstanceDevices(inst api.Instance, req api.DevLXDInstancePut, identityID string, isDeviceAccessible devLXDDeviceAccessValidator) (devices map[string]map[string]string, config map[string]string, err error) {
	// Ensure device access validator is provided.
	if isDeviceAccessible == nil {
		return nil, nil, api.StatusErrorf(http.StatusInternalServerError, "Missing device access validator")
	}

	// Pass local devices, as non-local devices cannot be owned.
	ownedDevices := getDevLXDOwnedDevices(inst.Devices, inst.Config, identityID)

	newDevices := make(map[string]map[string]string)
	newConfig := make(map[string]string)

	// Merge new devices into existing ones.
	for name, device := range req.Devices {
		ownedDevice, isOwned := ownedDevices[name]

		if device == nil {
			// Device is being removed. Check if the device is owned.
			// For consistency with LXD API, we do not error out if
			// the device is not found.
			if !isOwned {
				continue
			}

			// Ensure devLXD has sufficient permissions to manage the device.
			// Pass old device to the validator, as new device is nil.
			if !isDeviceAccessible(ownedDevice) {
				return nil, nil, api.StatusErrorf(http.StatusForbidden, "Not authorized to delete device %q", name)
			}

			// Device is removed, so remove the owner config key.
			newConfig["volatile."+name+".devlxd.owner"] = ""
		} else {
			_, exists := inst.ExpandedDevices[name]

			// Device is being either added or updated.
			// Ensure devLXD has sufficient permissions to manage the device.
			// If the device already exists (update), ensure that it is owned.
			if (exists && !isOwned) || (!isDeviceAccessible(device)) {
				return nil, nil, api.StatusErrorf(http.StatusForbidden, "Not authorized to manage device %q", name)
			}

			// At this point we know that the ownership is correct (there is
			// no existing unowned device), so we can safely set the device owner
			// in instance configuration.
			newConfig["volatile."+name+".devlxd.owner"] = identityID
		}

		newDevices[name] = device
	}

	return newDevices, newConfig, nil
}

// getDevLXDOwnedDevices extracts instance devices that are owned by the provided identity.
func getDevLXDOwnedDevices(devices map[string]map[string]string, config map[string]string, identityID string) map[string]map[string]string {
	ownedDevices := make(map[string]map[string]string)

	for name, device := range devices {
		if config["volatile."+name+".devlxd.owner"] == identityID {
			ownedDevices[name] = device
		}
	}

	return ownedDevices
}

// newDevLXDDeviceAccessValidator returns a device validator function that
// checks if the given device is accessible by the devLXD.
//
// For example, disk device is accessible if the appropriate security flag
// is enabled on the instance and the device represents a custom volume.
func newDevLXDDeviceAccessValidator(inst instance.Instance) devLXDDeviceAccessValidator {
	diskDeviceAllowed := hasInstanceSecurityFeatures(inst.ExpandedConfig(), devLXDSecurityManagementVolumesKey)

	return func(device map[string]string) bool {
		return diskDeviceAllowed && filters.IsCustomVolumeDisk(device)
	}
}
