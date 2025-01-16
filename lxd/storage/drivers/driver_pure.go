package drivers

import (
	"fmt"
	"net/http"
	"os/exec"
	"strings"

	"github.com/canonical/lxd/lxd/migration"
	"github.com/canonical/lxd/lxd/operations"
	"github.com/canonical/lxd/shared"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/revert"
	"github.com/canonical/lxd/shared/units"
	"github.com/canonical/lxd/shared/validate"
)

// pureLoaded indicates whether load() function was already called for the Pure Storage driver.
var pureLoaded = false

// pureVersion indicates Pure Storage version.
var pureVersion = ""

// Pure Storage modes.
const (
	pureModeISCSI = "iscsi"
)

type pure struct {
	common

	// Holds the low level HTTP client for the Pure Storage API.
	// Use pure.client() to retrieve the client struct.
	httpClient *pureClient

	// apiVersion indicates the Pure Storage API version.
	apiVersion string
}

// load is used initialize the driver. It should be used only once.
func (d *pure) load() error {
	// Done if previously loaded.
	if pureLoaded {
		return nil
	}

	var versions []string

	// Extract iscsiadm version, if it is installed on the host.
	_, err := exec.LookPath("iscsiadm")
	if err == nil {
		// Detect and record the version of the iSCSI CLI.
		// It will fail if the "iscsiadm" is not installed on the host.
		out, err := shared.RunCommand("iscsiadm", "--version")
		if err != nil {
			return fmt.Errorf("Failed to get iscsiadm version: %w", err)
		}

		fields := strings.Split(strings.TrimSpace(out), " ")
		if strings.HasPrefix(out, "iscsiadm version ") && len(fields) > 2 {
			versions = append(versions, fmt.Sprintf("%s (iscsiadm)", fields[2]))
		}

		// Load the iSCSI and kernel modules, ignoring those that cannot be loaded.
		// Support for the Pure Storage mode is checked during pool creation. However, this
		// ensures that the kernel modules are loaded, even if the host has been rebooted.
		_ = d.loadISCSIModules()
	}

	pureVersion = strings.Join(versions, " / ")
	pureLoaded = true

	return nil
}

// client returns the drivers Pure Storage client. A new client is created only if it does not already exist.
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
		DefaultBlockSize:             d.defaultBlockVolumeSize(),
		DefaultVMBlockFilesystemSize: d.defaultVMBlockFilesystemSize(),
		OptimizedImages:              false,
		PreservesInodes:              false,
		Remote:                       d.isRemote(),
		VolumeTypes:                  []VolumeType{VolumeTypeCustom, VolumeTypeVM, VolumeTypeContainer, VolumeTypeImage},
		BlockBacking:                 true,
		RunningCopyFreeze:            true,
		DirectIO:                     true,
		IOUring:                      true,
		MountedRoot:                  false,
		PopulateParentVolumeUUID:     true,
	}
}

// FillConfig populates the storage pool's configuration file with the default values.
func (d *pure) FillConfig() error {
	return nil
}

// Validate checks that all provided keys are supported and there is no conflicting or missing configuration.
func (d *pure) Validate(config map[string]string) error {
	rules := map[string]func(value string) error{
		"size": validate.Optional(validate.IsSize),
		// lxdmeta:generate(entities=storage-pure; group=pool-conf; key=pure.api.token)
		//
		// ---
		//  type: string
		//  shortdesc: API token for Pure Storage gateway authentication
		"pure.api.token": validate.Optional(),
		// lxdmeta:generate(entities=storage-pure; group=pool-conf; key=pure.gateway)
		//
		// ---
		//  type: string
		//  shortdesc: Address of the Pure Storage gateway
		"pure.gateway": validate.Optional(validate.IsRequestURL),
		// lxdmeta:generate(entities=storage-pure; group=pool-conf; key=pure.gateway.verify)
		//
		// ---
		//  type: bool
		//  defaultdesc: `true`
		//  shortdesc: Whether to verify the Pure Storage gateway's certificate
		"pure.gateway.verify": validate.Optional(validate.IsBool),
		// lxdmeta:generate(entities=storage-pure; group=pool-conf; key=pure.mode)
		// The mode to use to map Pure Storage volumes to the local server.
		// ---
		//  type: string
		//  defaultdesc: the discovered mode
		//  shortdesc: How volumes are mapped to the local server
		"pure.mode": validate.Optional(validate.IsOneOf(pureModeISCSI)),
		// lxdmeta:generate(entities=storage-pure; group=pool-conf; key=volume.size)
		// Default Pure Storage volume size rounded to 512B. The minimum size is 1MiB.
		// ---
		//  type: string
		//  defaultdesc: `10GiB`
		//  shortdesc: Size/quota of the storage volume
		"volume.size": validate.Optional(validate.IsMultipleOfUnit("512B")),
	}

	err := d.validatePool(config, rules, d.commonVolumeRules())
	if err != nil {
		return err
	}

	// Check if the selected Pure Storage mode is supported on this node.
	// Also when forming the storage pool on a LXD cluster, the mode
	// that got discovered on the creating machine needs to be validated
	// on the other cluster members too. This can be done here since Validate
	// gets executed on every cluster member when receiving the cluster
	// notification to finally create the pool.
	if config["pure.mode"] == pureModeISCSI {
		if !d.loadISCSIModules() {
			return fmt.Errorf("iSCSI is not supported")
		}
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

	revert := revert.New()
	defer revert.Fail()

	// Validate required Pure Storage configuration keys and return an error if they are
	// not set. Since those keys are not cluster member specific, the general validation
	// rules allow empty strings in order to create the pending storage pools.
	if d.config["pure.gateway"] == "" {
		return fmt.Errorf("The pure.gateway cannot be empty")
	}

	if d.config["pure.api.token"] == "" {
		return fmt.Errorf("The pure.api.token cannot be empty")
	}

	poolSizeBytes, err := units.ParseByteSizeString(d.config["size"])
	if err != nil {
		return fmt.Errorf("Failed to parse storage size: %w", err)
	}

	// Create the storage pool.
	err = d.client().createStoragePool(d.name, poolSizeBytes)
	if err != nil {
		return err
	}

	revert.Add(func() { _ = d.client().deleteStoragePool(d.name) })

	revert.Success()

	return nil
}

// Update applies any driver changes required from a configuration change.
func (d *pure) Update(changedConfig map[string]string) error {
	return nil
}

// Delete removes the storage pool (Pure Storage pod).
func (d *pure) Delete(op *operations.Operation) error {
	// First delete the storage pool on Pure Storage.
	err := d.client().deleteStoragePool(d.name)
	if err != nil && !api.StatusErrorCheck(err, http.StatusNotFound) {
		return err
	}

	// If the user completely destroyed it, call it done.
	if !shared.PathExists(GetPoolMountPath(d.name)) {
		return nil
	}

	// On delete, wipe everything in the directory.
	return wipeDirectory(GetPoolMountPath(d.name))
}

// Mount mounts the storage pool.
func (d *pure) Mount() (bool, error) {
	// Nothing to do here.
	return true, nil
}

// Unmount unmounts the storage pool.
func (d *pure) Unmount() (bool, error) {
	// Nothing to do here.
	return true, nil
}

// GetResources returns the pool resource usage information.
func (d *pure) GetResources() (*api.ResourcesStoragePool, error) {
	res := &api.ResourcesStoragePool{}
	return res, nil
}

// MigrationTypes returns the type of transfer methods to be used when doing migrations between pools in preference order.
func (d *pure) MigrationTypes(contentType ContentType, refresh bool, copySnapshots bool) []migration.Type {
	return []migration.Type{}
}
