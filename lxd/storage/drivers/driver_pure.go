package drivers

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/canonical/lxd/lxd/migration"
	"github.com/canonical/lxd/lxd/operations"
	"github.com/canonical/lxd/shared"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/logger"
	"github.com/canonical/lxd/shared/revert"
	"github.com/canonical/lxd/shared/validate"
)

// pureLoaded indicates whether load() function was already called for the PureStorage driver.
var pureLoaded = false

// pureVersion indicates PureStorage (iscsiadm) version.
var pureVersion = ""

// pureDefaultVolumeSize represents the default PureStorage volume size.
const pureDefaultVolumeSize = "10GiB"

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

	// apiVersion indicates the PureStorage API version.
	apiVersion string

	// iscsiTarget represents an iSCSI target that the initiator (host) can connect to.
	// It is used as cache to avoid unnecessary discovery.
	iscsiTarget *pureTarget
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
		OptimizedImages:              true,
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
	// Use iSCSI by default.
	if d.config["pure.mode"] == "" {
		d.config["pure.mode"] = pureModeISCSI
	}

	// Set default PureStorage volume size.
	if d.config["volume.size"] == "" {
		d.config["volume.size"] = pureDefaultVolumeSize
	}

	return nil
}

// Validate checks that all provided keys are supported and that no conflicting or missing configuration is present.
func (d *pure) Validate(config map[string]string) error {
	rules := map[string]func(value string) error{
		// lxdmeta:generate(entities=storage-pure; group=pool-conf; key=pure.user.name)
		//
		// ---
		//  type: string
		//  defaultdesc: `pureuser`
		//  shortdesc: User for PureStorage gateway authentication
		"pure.user.name": validate.IsAny,
		// lxdmeta:generate(entities=storage-pure; group=pool-conf; key=pure.user.password)
		//
		// ---
		//  type: string
		//  shortdesc: Password for PureStorage gateway authentication
		"pure.user.password": validate.IsAny,
		// lxdmeta:generate(entities=storage-pure; group=pool-conf; key=pure.user.password)
		//
		// ---
		//  type: string
		//  shortdesc: API token for PureStorage gateway authentication
		"pure.api.token": validate.IsNotEmpty,
		// lxdmeta:generate(entities=storage-pure; group=pool-conf; key=pure.gateway)
		//
		// ---
		//  type: string
		//  shortdesc: Address of the PureStorage Gateway
		"pure.gateway": validate.IsRequestURL,
		// lxdmeta:generate(entities=storage-pure; group=pool-conf; key=pure.gateway.verify)
		//
		// ---
		//  type: bool
		//  defaultdesc: `true`
		//  shortdesc: Whether to verify the PureStorage gateway's certificate
		"pure.gateway.verify": validate.Optional(validate.IsBool),
		// lxdmeta:generate(entities=storage-pure; group=pool-conf; key=pure.mode)
		// The mode to use to map PureStorage volumes to the local server.
		// Currently, only `iscsi` is supported.
		// ---
		//  type: string
		//  defaultdesc: the discovered mode
		//  shortdesc: How volumes are mapped to the local server
		"pure.mode": validate.Optional(validate.IsOneOf(pureModeISCSI)),
		// lxdmeta:generate(entities=storage-pure; group=pool-conf; key=pure.iscsi.address)
		//
		// ---
		//  type: string
		//  defaultdesc: the IP address of the iSCSI host
		//  shortdesc: The IP address of the PureStorage FlashArray iSCSI host.
		"pure.iscsi.address": validate.Optional(validate.IsNetworkAddress),
		// lxdmeta:generate(entities=storage-pure; group=pool-conf; key=volume.size)
		// Default PureStorage volume size rounded to 512B. The minimum size is 1MiB.
		// ---
		//  type: string
		//  defaultdesc: `8GiB`
		//  shortdesc: Size/quota of the storage volume
		"volume.size": validate.Optional(validate.IsMultipleOfUnit("512B")),
	}

	err := d.validatePool(config, rules, d.commonVolumeRules())
	if err != nil {
		return err
	}

	// Check if the selected PureStorage mode is supported on this node.
	// Also when forming the storage pool on a LXD cluster, the mode
	// that got discovered on the creating machine needs to be validated
	// on the other cluster members too. This can be done here since Validate
	// gets executed on every cluster member when receiving the cluster
	// notification to finally create the pool.
	if d.config["pure.mode"] == pureModeISCSI {
		if !d.loadISCSIModules() {
			return fmt.Errorf("iSCSI is not supported")
		}

		if d.config["pure.iscsi.address"] == "" {
			return fmt.Errorf("The pure.iscsi.address must be set when mode is set to iSCSI")
		}
	}

	return nil
}

// Create is called during pool creation and is effectively using an empty driver struct.
// WARNING: The Create() function cannot rely on any of the struct attributes being set.
//
// Creation process encompasses creation of storage pool and host. The host is created on each
// cluster member (if LXD is clustered) and is used to access all volumes on a remote PureStorage.
func (d *pure) Create() error {
	err := d.FillConfig()
	if err != nil {
		return err
	}

	err = d.Validate(d.config)
	if err != nil {
		return err
	}

	revert := revert.New()
	defer revert.Fail()

	switch d.config["pure.mode"] {
	case pureModeISCSI:
		// Nothing to do here (yet).
	default:
		return fmt.Errorf("Unsupported PureStorage mode %q", d.config["pure.mode"])
	}

	// Create the storage pool.
	err = d.client().createStoragePool(d.name)
	if err != nil {
		return fmt.Errorf("Failed to create storage pool: %w", err)
	}

	revert.Add(func() { _ = d.client().deleteStoragePool(d.name) })

	logger.Info("Storage pool successfully created", logger.Ctx{"name": d.name, "api_version": d.apiVersion})

	revert.Success()
	return nil
}

// Update applies any driver changes required from a configuration change.
func (d *pure) Update(changedConfig map[string]string) error {
	return nil
}

// Delete removes the storage pool (PureStorage Pod).
func (d *pure) Delete(op *operations.Operation) error {
	// First delete the storage pool on PureStorage.
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
	pool, err := d.client().getStoragePool(d.name)
	if err != nil {
		return nil, err
	}

	logger.Warn("Pool resources", logger.Ctx{"pool": d.name, "space.total": pool.Space.TotalBytes, "space.used": pool.Space.UsedBytes})

	res := &api.ResourcesStoragePool{}

	res.Space.Total = uint64(pool.Quota)
	res.Space.Used = uint64(pool.Space.UsedBytes)

	if pool.Quota == 0 {
		// If quota is set to 0, it means that the storage pool is unbounded. Therefore,
		// collect the total capacity of arrays where storage pool provisioned.
		arrayNames := make([]string, 0, len(pool.Arrays))
		for _, array := range pool.Arrays {
			arrayNames = append(arrayNames, array.Name)
		}

		arrays, err := d.client().getStorageArrays(arrayNames...)
		if err != nil {
			return nil, err
		}

		for _, array := range arrays {
			res.Space.Total += uint64(array.Capacity)
		}
	}

	return res, nil
}

// MigrationTypes returns the type of transfer methods to be used when doing migrations between pools in preference order.
func (d *pure) MigrationTypes(contentType ContentType, refresh bool, copySnapshots bool) []migration.Type {
	return []migration.Type{}
}
