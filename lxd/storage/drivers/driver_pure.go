package drivers

import (
	"fmt"
	"strings"

	"github.com/canonical/lxd/shared"
)

// pureLoaded indicates whether load() function was already called for the PureStorage driver.
var pureLoaded = false

// pureVersion indicates PureStorage (iscsiadm) version.
var pureVersion = ""

// pureDefaultVolumeSize represents the default PureStorage volume size.
const pureDefaultVolumeSize = "8GiB"

// PureStorage modes.
const (
	pureModeISCSI = "iscsi"
	// pureModeNVMeTCP = "nvme-tcp"
)

type pure struct {
	common

	// Holds the low level HTTP client for the PureStorage API.
	// Use pure.client() to retrieve the client struct.
	httpClient *pureClient
}

// load is used initialize the driver. It should be used only once.
func (d *pure) load() error {
	// Done if previously loaded.
	if pureLoaded {
		return nil
	}

	// Detect and record the version of the iSCSI CLI.
	// The iSCSI CLI is shipped with the snap.
	out, err := shared.RunCommand("iscsiadm", "--version")
	if err != nil {
		return fmt.Errorf("Failed to get iscsiadm version: %w", err)
	}

	fields := strings.Split(strings.TrimSpace(out), " ")
	if strings.HasPrefix(out, "iscsiadm version ") && len(fields) > 2 {
		pureVersion = fmt.Sprintf("%s (iscsiadm)", fields[2])
	}

	// Load the iSCSI kernel modules, ignoring those that cannot be loaded.
	// Support for the iSCSI mode is checked during pool creation. However,
	// this ensures that the kernel modules are loaded, even if the host
	// has been rebooted.
	_ = d.loadISCSIModules()

	pureLoaded = true
	return nil
}

// client returns the drivers PureStorage client. A new client is created only if it does not already exist.
func (d *pure) client() *pureClient {
	if d.httpClient == nil {
		d.httpClient = newPureClient(d)
	}

	return d.httpClient
}

// isRemote returns true indicating this driver uses remote storage.
func (d *pure) isRemote() bool {
	return true
}

// Info returns info about the driver and its environment.
func (d *pure) Info() Info {
	return Info{
		Name:                         "pure",
		Version:                      pureVersion,
		DefaultVMBlockFilesystemSize: "",
		OptimizedImages:              false,
		PreservesInodes:              false,
		Remote:                       d.isRemote(),
		VolumeTypes:                  []VolumeType{VolumeTypeCustom, VolumeTypeVM, VolumeTypeContainer, VolumeTypeImage},
		BlockBacking:                 true,
		RunningCopyFreeze:            true,
		DirectIO:                     true,
		IOUring:                      true,
		MountedRoot:                  false,
	}
}

// FillConfig populates the storage pool's configuration file with the default values.
func (d *pure) FillConfig() error {
	// Use NVMe/tcp by default.
	if d.config["pure.mode"] == "" {
		d.config["pure.mode"] = pureModeISCSI
	}

	// Set default PureStorage volume size.
	if d.config["volume.size"] == "" {
		d.config["volume.size"] = pureDefaultVolumeSize
	}

	return nil
}

// Create is called during pool creation and is effectively using an empty driver struct.
// WARNING: The Create() function cannot rely on any of the struct attributes being set.
func (d *pure) Create() error {
	err := d.FillConfig()
	if err != nil {
		return err
	}

	// Ensure pool name is set.
	if d.config["pure.pool"] == "" {
		return fmt.Errorf("The pure.pool cannot be empty")
	}

	// Ensure PureStorage gateway address is set.
	if d.config["pure.gateway"] == "" {
		return fmt.Errorf("The pure.gateway cannot be empty")
	}

	// Ensure PureStorage API token is provided.
	if d.config["pure.api.token"] == "" {
		return fmt.Errorf("PureStorage API token cannot be empty")
	}

	// client := d.client()

	switch d.config["pure.mode"] {
	case pureModeISCSI:
		// Discover one of the storage pools SDT services.
		// if d.config["powerflex.sdt"] == "" {
		// 	pool, err := d.resolvePool()
		// 	if err != nil {
		// 		return err
		// 	}

		// 	relations, err := client.getProtectionDomainSDTRelations(pool.ProtectionDomainID)
		// 	if err != nil {
		// 		return err
		// 	}

		// 	if len(relations) == 0 {
		// 		return fmt.Errorf("Failed to retrieve at least one SDT for the given storage pool: %q", pool.ID)
		// 	}

		// 	if len(relations[0].IPList) == 0 {
		// 		return fmt.Errorf("Failed to retrieve IP from SDT: %q", relations[0].Name)
		// 	}

		// 	d.config["powerflex.sdt"] = relations[0].IPList[0].IP
		// }
	default:
		return fmt.Errorf("Unsupported PureStorage mode %q", d.config["pure.mode"])
	}

	return nil
}
