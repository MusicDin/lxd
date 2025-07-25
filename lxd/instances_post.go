package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"

	petname "github.com/dustinkirkland/golang-petname"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/canonical/lxd/lxd/archive"
	"github.com/canonical/lxd/lxd/auth"
	"github.com/canonical/lxd/lxd/backup"
	"github.com/canonical/lxd/lxd/cluster"
	"github.com/canonical/lxd/lxd/db"
	dbCluster "github.com/canonical/lxd/lxd/db/cluster"
	"github.com/canonical/lxd/lxd/db/operationtype"
	deviceConfig "github.com/canonical/lxd/lxd/device/config"
	"github.com/canonical/lxd/lxd/instance"
	"github.com/canonical/lxd/lxd/instance/instancetype"
	"github.com/canonical/lxd/lxd/instance/operationlock"
	"github.com/canonical/lxd/lxd/operations"
	"github.com/canonical/lxd/lxd/project"
	"github.com/canonical/lxd/lxd/project/limits"
	"github.com/canonical/lxd/lxd/request"
	"github.com/canonical/lxd/lxd/response"
	"github.com/canonical/lxd/lxd/scriptlet"
	"github.com/canonical/lxd/lxd/state"
	storagePools "github.com/canonical/lxd/lxd/storage"
	"github.com/canonical/lxd/shared"
	"github.com/canonical/lxd/shared/api"
	apiScriptlet "github.com/canonical/lxd/shared/api/scriptlet"
	"github.com/canonical/lxd/shared/logger"
	"github.com/canonical/lxd/shared/osarch"
	"github.com/canonical/lxd/shared/revert"
	"github.com/canonical/lxd/shared/version"
)

func ensureDownloadedImageFitWithinBudget(ctx context.Context, s *state.State, op *operations.Operation, p api.Project, imgAlias string, source api.InstanceSource, imgType string) (*api.Image, error) {
	var autoUpdate bool
	var err error
	if p.Config["images.auto_update_cached"] != "" {
		autoUpdate = shared.IsTrue(p.Config["images.auto_update_cached"])
	} else {
		autoUpdate = s.GlobalConfig.ImagesAutoUpdateCached()
	}

	var budget int64
	err = s.DB.Cluster.Transaction(s.ShutdownCtx, func(ctx context.Context, tx *db.ClusterTx) error {
		budget, err = limits.GetImageSpaceBudget(ctx, s.GlobalConfig, tx, p.Name)
		return err
	})
	if err != nil {
		return nil, err
	}

	imgDownloaded, err := ImageDownload(ctx, s, op, &ImageDownloadArgs{
		Server:       source.Server,
		Protocol:     source.Protocol,
		Certificate:  source.Certificate,
		Secret:       source.Secret,
		Alias:        imgAlias,
		SetCached:    true,
		Type:         imgType,
		AutoUpdate:   autoUpdate,
		Public:       false,
		PreferCached: true,
		ProjectName:  p.Name,
		Budget:       budget,
	})
	if err != nil {
		return nil, err
	}

	return imgDownloaded, nil
}

func createFromImage(ctx context.Context, s *state.State, p api.Project, profiles []api.Profile, img *api.Image, imgAlias string, req *api.InstancesPost) response.Response {
	if s.DB.Cluster.LocalNodeIsEvacuated() {
		return response.Forbidden(errors.New("Cluster member is evacuated"))
	}

	dbType, err := instancetype.New(string(req.Type))
	if err != nil {
		return response.BadRequest(err)
	}

	run := func(op *operations.Operation) error {
		devices := deviceConfig.NewDevices(req.Devices)

		args := db.InstanceArgs{
			Project:     p.Name,
			Config:      req.Config,
			Type:        dbType,
			Description: req.Description,
			Devices:     deviceConfig.ApplyDeviceInitialValues(devices, profiles),
			Ephemeral:   req.Ephemeral,
			Name:        req.Name,
			Profiles:    profiles,
		}

		if req.Source.Server != "" {
			img, err = ensureDownloadedImageFitWithinBudget(ctx, s, op, p, imgAlias, req.Source, string(req.Type))
			if err != nil {
				return err
			}
		} else if img != nil {
			err := ensureImageIsLocallyAvailable(ctx, s, img, args.Project)
			if err != nil {
				return err
			}
		} else {
			return errors.New("Image not provided for instance creation")
		}

		args.Architecture, err = osarch.ArchitectureId(img.Architecture)
		if err != nil {
			return err
		}

		// Actually create the instance.
		err = instanceCreateFromImage(s, img, args, op)
		if err != nil {
			return err
		}

		return instanceCreateFinish(s, req, args)
	}

	resources := map[string][]api.URL{}
	resources["instances"] = []api.URL{*api.NewURL().Path(version.APIVersion, "instances", req.Name)}

	if dbType == instancetype.Container {
		resources["containers"] = resources["instances"]
	}

	op, err := operations.OperationCreate(ctx, s, p.Name, operations.OperationClassTask, operationtype.InstanceCreate, resources, nil, run, nil, nil)
	if err != nil {
		return response.InternalError(err)
	}

	return operations.OperationResponse(op)
}

func createFromNone(ctx context.Context, s *state.State, projectName string, profiles []api.Profile, req *api.InstancesPost) response.Response {
	if s.DB.Cluster.LocalNodeIsEvacuated() {
		return response.Forbidden(errors.New("Cluster member is evacuated"))
	}

	dbType, err := instancetype.New(string(req.Type))
	if err != nil {
		return response.BadRequest(err)
	}

	devices := deviceConfig.NewDevices(req.Devices)

	args := db.InstanceArgs{
		Project:     projectName,
		Config:      req.Config,
		Type:        dbType,
		Description: req.Description,
		Devices:     deviceConfig.ApplyDeviceInitialValues(devices, profiles),
		Ephemeral:   req.Ephemeral,
		Name:        req.Name,
		Profiles:    profiles,
	}

	if req.Architecture != "" {
		architecture, err := osarch.ArchitectureId(req.Architecture)
		if err != nil {
			return response.InternalError(err)
		}

		args.Architecture = architecture
	}

	run := func(_ *operations.Operation) error {
		// Actually create the instance.
		_, err := instanceCreateAsEmpty(s, args)
		if err != nil {
			return err
		}

		return instanceCreateFinish(s, req, args)
	}

	resources := map[string][]api.URL{}
	resources["instances"] = []api.URL{*api.NewURL().Path(version.APIVersion, "instances", req.Name)}

	if dbType == instancetype.Container {
		resources["containers"] = resources["instances"]
	}

	op, err := operations.OperationCreate(ctx, s, projectName, operations.OperationClassTask, operationtype.InstanceCreate, resources, nil, run, nil, nil)
	if err != nil {
		return response.InternalError(err)
	}

	return operations.OperationResponse(op)
}

func createFromMigration(ctx context.Context, s *state.State, projectName string, profiles []api.Profile, req *api.InstancesPost, isClusterNotification bool) response.Response {
	reqInfo := request.GetContextInfo(ctx)
	if reqInfo != nil {
		if reqInfo.Protocol == "" {
			return response.SmartError(errors.New("Failed to check request origin: Protocol not set in request context"))
		}

		// If the protocol is not auth.AuthenticationMethodCluster (e.g. not an internal request) and the node has been
		// evacuated, reject the request.
		if s.DB.Cluster.LocalNodeIsEvacuated() && reqInfo.Protocol != auth.AuthenticationMethodCluster {
			return response.Forbidden(errors.New("Cluster member is evacuated"))
		}
	}

	// Validate migration mode.
	if req.Source.Mode != "pull" && req.Source.Mode != "push" {
		return response.NotImplemented(fmt.Errorf("Mode %q not implemented", req.Source.Mode))
	}

	dbType, err := instancetype.New(string(req.Type))
	if err != nil {
		return response.BadRequest(err)
	}

	if dbType != instancetype.Container && dbType != instancetype.VM {
		return response.BadRequest(fmt.Errorf("Instance type not supported %q", req.Type))
	}

	storagePool, args, resp := setupInstanceArgs(s, dbType, projectName, profiles, req)
	if resp != nil {
		return resp
	}

	var inst instance.Instance
	var instOp *operationlock.InstanceOperation
	var cleanup revert.Hook

	// Decide if this is an internal cluster move request.
	var clusterMoveSourceName string
	if isClusterNotification {
		if req.Source.Source == "" {
			return response.BadRequest(errors.New("Source instance name must be provided for cluster member move"))
		}

		clusterMoveSourceName = req.Source.Source
	}

	// Early check for refresh and cluster same name move to check instance exists.
	if req.Source.Refresh || (clusterMoveSourceName != "" && clusterMoveSourceName == req.Name) {
		inst, err = instance.LoadByProjectAndName(s, projectName, req.Name)
		if err != nil {
			if !response.IsNotFoundError(err) {
				return response.SmartError(err)
			}

			if clusterMoveSourceName != "" {
				// Cluster move doesn't allow renaming as part of migration so fail here.
				return response.SmartError(errors.New("Cluster move doesn't allow renaming"))
			}

			req.Source.Refresh = false
		}
	}

	revert := revert.New()
	defer revert.Fail()

	// We keep the ContainerOnly for backward compatibility.
	instanceOnly := req.Source.InstanceOnly || req.Source.ContainerOnly //nolint:staticcheck,unused

	if inst == nil {
		_, err := storagePools.LoadByName(s, storagePool)
		if err != nil {
			return response.InternalError(err)
		}

		// Create the instance DB record for main instance.
		// Note: At this stage we do not yet know if snapshots are going to be received and so we cannot
		// create their DB records. This will be done if needed in the migrationSink.Do() function called
		// as part of the operation below.
		inst, instOp, cleanup, err = instance.CreateInternal(s, *args, true)
		if err != nil {
			return response.InternalError(fmt.Errorf("Failed creating instance record: %w", err))
		}

		revert.Add(cleanup)
	} else {
		instOp, err = inst.LockExclusive()
		if err != nil {
			return response.SmartError(fmt.Errorf("Failed getting exclusive access to instance: %w", err))
		}
	}

	revert.Add(func() { instOp.Done(err) })

	push := false
	var dialer *websocket.Dialer

	if req.Source.Mode == "push" {
		push = true
	} else {
		dialer, err = setupWebsocketDialer(req.Source.Certificate)
		if err != nil {
			return response.SmartError(fmt.Errorf("Failed setting up websocket dialer for migration sink connections: %w", err))
		}
	}

	migrationArgs := migrationSinkArgs{
		url:                   req.Source.Operation,
		dialer:                dialer,
		instance:              inst,
		secrets:               req.Source.Websockets,
		push:                  push,
		live:                  req.Source.Live,
		instanceOnly:          instanceOnly,
		clusterMoveSourceName: clusterMoveSourceName,
		refresh:               req.Source.Refresh,
	}

	sink, err := newMigrationSink(&migrationArgs)
	if err != nil {
		return response.InternalError(err)
	}

	// Copy reverter so far so we can use it inside run after this function has finished.
	runRevert := revert.Clone()

	run := func(op *operations.Operation) error {
		defer runRevert.Fail()

		sink.instance.SetOperation(op)

		// And finally run the migration.
		err = sink.Do(s, instOp)
		if err != nil {
			err = fmt.Errorf("Error transferring instance data: %w", err)
			instOp.Done(err) // Complete operation that was created earlier, to release lock.

			return err
		}

		instOp.Done(nil) // Complete operation that was created earlier, to release lock.
		runRevert.Success()
		return nil
	}

	resources := map[string][]api.URL{}
	resources["instances"] = []api.URL{*api.NewURL().Path(version.APIVersion, "instances", req.Name)}

	if dbType == instancetype.Container {
		resources["containers"] = resources["instances"]
	}

	var op *operations.Operation
	if push {
		op, err = operations.OperationCreate(ctx, s, projectName, operations.OperationClassWebsocket, operationtype.InstanceCreate, resources, sink.Metadata(), run, nil, sink.Connect)
		if err != nil {
			return response.InternalError(err)
		}
	} else {
		op, err = operations.OperationCreate(ctx, s, projectName, operations.OperationClassTask, operationtype.InstanceCreate, resources, nil, run, nil, nil)
		if err != nil {
			return response.InternalError(err)
		}
	}

	revert.Success()
	return operations.OperationResponse(op)
}

// createFromConversion receives the root disk (container FS or VM block volume) from the client and creates an
// instance from it. Conversion options also allow the uploaded image to be converted into a raw format.
func createFromConversion(ctx context.Context, s *state.State, projectName string, profiles []api.Profile, req *api.InstancesPost) response.Response {
	if s.DB.Cluster.LocalNodeIsEvacuated() {
		return response.Forbidden(errors.New("Cluster member is evacuated"))
	}

	// Validate migration mode.
	if req.Source.Mode != "push" {
		return response.NotImplemented(fmt.Errorf("Mode %q not implemented", req.Source.Mode))
	}

	dbType, err := instancetype.New(string(req.Type))
	if err != nil {
		return response.BadRequest(err)
	}

	// Only virtual machines support additional conversion options.
	if dbType != instancetype.VM && len(req.Source.ConversionOptions) > 0 {
		return response.BadRequest(fmt.Errorf("Conversion options can only be used with virtual machines. Instance type %q does not support conversion options", req.Type))
	}

	// Validate conversion options.
	for _, opt := range req.Source.ConversionOptions {
		if !slices.Contains([]string{"format", "virtio"}, opt) {
			return response.BadRequest(fmt.Errorf("Invalid conversion option %q", opt))
		}
	}

	storagePool, args, resp := setupInstanceArgs(s, dbType, projectName, profiles, req)
	if resp != nil {
		return resp
	}

	revert := revert.New()
	defer revert.Fail()

	_, err = storagePools.LoadByName(s, storagePool)
	if err != nil {
		return response.InternalError(err)
	}

	// Create the instance DB record for main instance.
	inst, instOp, cleanup, err := instance.CreateInternal(s, *args, true)
	if err != nil {
		return response.InternalError(fmt.Errorf("Failed creating instance record: %w", err))
	}

	revert.Add(cleanup)
	if err != nil {
		return response.SmartError(fmt.Errorf("Failed getting exclusive access to instance: %w", err))
	}

	revert.Add(func() { instOp.Done(err) })

	conversionArgs := conversionSinkArgs{
		url:               req.Source.Operation,
		secrets:           req.Source.Websockets,
		sourceDiskSize:    req.Source.SourceDiskSize,
		conversionOptions: req.Source.ConversionOptions,
		instance:          inst,
	}

	sink, err := newConversionSink(&conversionArgs)
	if err != nil {
		return response.InternalError(err)
	}

	// Copy reverter so far so we can use it inside run after this function has finished.
	runRevert := revert.Clone()

	run := func(op *operations.Operation) error {
		defer runRevert.Fail()

		sink.instance.SetOperation(op)

		// And finally run the migration.
		err = sink.Do(s, instOp)
		if err != nil {
			err = fmt.Errorf("Error transferring instance data: %w", err)
			instOp.Done(err) // Complete operation that was created earlier, to release lock.

			return err
		}

		instOp.Done(nil) // Complete operation that was created earlier, to release lock.
		runRevert.Success()
		return nil
	}

	resources := map[string][]api.URL{}
	resources["instances"] = []api.URL{*api.NewURL().Path(version.APIVersion, "instances", req.Name)}

	if dbType == instancetype.Container {
		resources["containers"] = resources["instances"]
	}

	op, err := operations.OperationCreate(ctx, s, projectName, operations.OperationClassWebsocket, operationtype.InstanceCreate, resources, sink.Metadata(), run, nil, sink.Connect)
	if err != nil {
		return response.InternalError(err)
	}

	revert.Success()
	return operations.OperationResponse(op)
}

func createFromCopy(ctx context.Context, s *state.State, projectName string, profiles []api.Profile, req *api.InstancesPost) response.Response {
	if s.DB.Cluster.LocalNodeIsEvacuated() {
		return response.Forbidden(errors.New("Cluster member is evacuated"))
	}

	if req.Source.Source == "" {
		return response.BadRequest(errors.New("Must specify a source instance"))
	}

	sourceProject := req.Source.Project
	if sourceProject == "" {
		sourceProject = projectName
	}

	targetProject := projectName

	source, err := instance.LoadByProjectAndName(s, sourceProject, req.Source.Source)
	if err != nil {
		return response.SmartError(err)
	}

	// When clustered, use the node name, otherwise use the hostname.
	if s.ServerClustered {
		serverName := s.ServerName

		if serverName != source.Location() {
			// Check if we are copying from a ceph-based container.
			_, rootDevice, _ := instancetype.GetRootDiskDevice(source.ExpandedDevices().CloneNative())
			sourcePoolName := rootDevice["pool"]

			destPoolName, _, _, _, resp := instanceFindStoragePool(s, targetProject, req)
			if resp != nil {
				return resp
			}

			if sourcePoolName != destPoolName {
				// Redirect to migration
				return clusterCopyContainerInternal(ctx, s, source, projectName, profiles, req)
			}

			var pool *api.StoragePool

			err = s.DB.Cluster.Transaction(ctx, func(ctx context.Context, tx *db.ClusterTx) error {
				_, pool, _, err = tx.GetStoragePoolInAnyState(ctx, sourcePoolName)

				return err
			})
			if err != nil {
				err = fmt.Errorf("Failed to fetch instance's pool info: %w", err)
				return response.SmartError(err)
			}

			if pool.Driver != "ceph" {
				// Redirect to migration
				return clusterCopyContainerInternal(ctx, s, source, projectName, profiles, req)
			}
		}
	}

	// Config override
	sourceConfig := source.LocalConfig()
	if req.Config == nil {
		req.Config = make(map[string]string)
	}

	for key, value := range sourceConfig {
		if !instancetype.InstanceIncludeWhenCopying(key, false) {
			logger.Debug("Skipping key from copy source", logger.Ctx{"key": key, "sourceProject": source.Project().Name, "sourceInstance": source.Name(), "project": targetProject, "instance": req.Name})
			continue
		}

		_, exists := req.Config[key]
		if exists {
			continue
		}

		req.Config[key] = value
	}

	// Devices override
	sourceDevices := source.LocalDevices()

	if req.Devices == nil {
		req.Devices = make(map[string]map[string]string)
	}

	for key, value := range sourceDevices {
		_, exists := req.Devices[key]
		if exists {
			continue
		}

		req.Devices[key] = value
	}

	if req.Stateful {
		sourceName, _, _ := api.GetParentAndSnapshotName(source.Name())
		if sourceName != req.Name {
			return response.BadRequest(fmt.Errorf("Instance name cannot be changed during stateful copy (%q to %q)", sourceName, req.Name))
		}
	}

	dbType, err := instancetype.New(string(req.Type))
	if err != nil {
		return response.BadRequest(err)
	}

	// If type isn't specified, match the source type.
	if req.Type == "" {
		dbType = source.Type()
	}

	if dbType != instancetype.Any && dbType != source.Type() {
		return response.BadRequest(errors.New("Instance type should not be specified or should match source type"))
	}

	args := db.InstanceArgs{
		Project:      targetProject,
		Architecture: source.Architecture(),
		BaseImage:    req.Source.BaseImage,
		Config:       req.Config,
		Type:         source.Type(),
		Description:  req.Description,
		Devices:      deviceConfig.NewDevices(req.Devices),
		Ephemeral:    req.Ephemeral,
		Name:         req.Name,
		Profiles:     profiles,
		Stateful:     req.Stateful,
	}

	run := func(op *operations.Operation) error {
		// Actually create the instance.
		_, err := instanceCreateAsCopy(s, instanceCreateAsCopyOpts{
			sourceInstance: source,
			targetInstance: args,
			// We keep the ContainerOnly for backward compatibility.
			instanceOnly:             req.Source.InstanceOnly || req.Source.ContainerOnly, //nolint:staticcheck,unused
			refresh:                  req.Source.Refresh,
			applyTemplateTrigger:     true,
			allowInconsistent:        req.Source.AllowInconsistent,
			overrideSnapshotProfiles: req.Source.OverrideSnapshotProfiles,
		}, op)
		if err != nil {
			return err
		}

		return instanceCreateFinish(s, req, args)
	}

	resources := map[string][]api.URL{}
	resources["instances"] = []api.URL{*api.NewURL().Path(version.APIVersion, "instances", req.Name)}

	if shared.IsSnapshot(req.Source.Source) {
		cName, sName, _ := api.GetParentAndSnapshotName(req.Source.Source)
		resources["instances_snapshots"] = []api.URL{*api.NewURL().Path(version.APIVersion, "instances", cName, "snapshots", sName)}
	} else {
		resources["instances"] = append(resources["instances"], *api.NewURL().Path(version.APIVersion, "instances", req.Source.Source))
	}

	if dbType == instancetype.Container {
		resources["containers"] = resources["instances"]
	}

	op, err := operations.OperationCreate(ctx, s, targetProject, operations.OperationClassTask, operationtype.InstanceCreate, resources, nil, run, nil, nil)
	if err != nil {
		return response.InternalError(err)
	}

	return operations.OperationResponse(op)
}

func createFromBackup(s *state.State, r *http.Request, projectName string, data io.Reader, pool string, instanceName string, devices map[string]map[string]string) response.Response {
	revert := revert.New()
	defer revert.Fail()

	backupsPath := s.BackupsStoragePath()

	// Create temporary file to store uploaded backup data.
	backupFile, err := os.CreateTemp(backupsPath, backup.WorkingDirPrefix+"_")
	if err != nil {
		return response.InternalError(err)
	}

	defer func() { _ = os.Remove(backupFile.Name()) }()
	revert.Add(func() { _ = backupFile.Close() })

	// Stream uploaded backup data into temporary file.
	_, err = io.Copy(backupFile, data)
	if err != nil {
		return response.InternalError(err)
	}

	// Detect squashfs compression and convert to tarball.
	_, err = backupFile.Seek(0, io.SeekStart)
	if err != nil {
		return response.InternalError(err)
	}

	_, algo, decomArgs, err := shared.DetectCompressionFile(backupFile)
	if err != nil {
		return response.InternalError(err)
	}

	if algo == ".squashfs" {
		// Pass the temporary file as program argument to the decompression command.
		decomArgs := append(decomArgs, backupFile.Name())

		// Create temporary file to store the decompressed tarball in.
		tarFile, err := os.CreateTemp(backupsPath, backup.WorkingDirPrefix+"_decompress_")
		if err != nil {
			return response.InternalError(err)
		}

		defer func() { _ = os.Remove(tarFile.Name()) }()

		// Decompress to tarFile temporary file.
		err = archive.ExtractWithFds(s, decomArgs[0], decomArgs[1:], nil, nil, tarFile)
		if err != nil {
			return response.InternalError(err)
		}

		// We don't need the original squashfs file anymore.
		_ = backupFile.Close()
		_ = os.Remove(backupFile.Name())

		// Replace the backup file handle with the handle to the tar file.
		backupFile = tarFile
	}

	// Parse the backup information.
	_, err = backupFile.Seek(0, io.SeekStart)
	if err != nil {
		return response.InternalError(err)
	}

	logger.Debug("Reading backup file info")
	bInfo, err := backup.GetInfo(s, backupFile, backupFile.Name())
	if err != nil {
		return response.BadRequest(err)
	}

	if bInfo.Config == nil {
		return response.BadRequest(errors.New("Backup config is missing"))
	}

	if bInfo.Config.Instance == nil {
		return response.BadRequest(errors.New("Instance definition in backup config is missing"))
	}

	// Check project permissions.
	var req api.InstancesPost
	err = s.DB.Cluster.Transaction(s.ShutdownCtx, func(ctx context.Context, tx *db.ClusterTx) error {
		req = api.InstancesPost{
			InstancePut: bInfo.Config.Instance.Writable(),
			Name:        bInfo.Name,
			Source:      api.InstanceSource{}, // Only relevant for "copy" or "migration", but may not be nil.
			Type:        api.InstanceType(bInfo.Config.Instance.Type),
		}

		return limits.AllowInstanceCreation(ctx, s.GlobalConfig, tx, projectName, req)
	})
	if err != nil {
		return response.SmartError(err)
	}

	bInfo.Project = projectName

	// Override pool.
	if pool != "" {
		bInfo.Pool = pool
	}

	// Override instance name.
	if instanceName != "" {
		bInfo.Name = instanceName
	}

	rootVol, err := bInfo.Config.RootVolume()
	if err != nil {
		return response.SmartError(fmt.Errorf("Failed getting the root volume: %w", err))
	}

	// Override the volume's UUID.
	// Normally a volume (and its snapshots) gets a new UUID if their config doesn't already have
	// a `volatile.uuid` field during creation of the volume's record in the DB.
	// When importing a backup we have to ensure to not pass the backup volume's UUID when
	// calling the actual backend functions for the target volume that perform some preliminary validation checks.
	rootVol.Config["volatile.uuid"] = uuid.New().String()

	// Override the volume snapshot's UUID.
	for _, snapshot := range rootVol.Snapshots {
		snapshot.Config["volatile.uuid"] = uuid.New().String()
	}

	logger.Debug("Backup file info loaded", logger.Ctx{
		"type":      bInfo.Type,
		"name":      bInfo.Name,
		"project":   bInfo.Project,
		"backend":   bInfo.Backend,
		"pool":      bInfo.Pool,
		"optimized": *bInfo.OptimizedStorage,
		"snapshots": bInfo.Snapshots,
	})

	err = s.DB.Cluster.Transaction(context.TODO(), func(ctx context.Context, tx *db.ClusterTx) error {
		// Check storage pool exists.
		_, _, _, err = tx.GetStoragePoolInAnyState(ctx, bInfo.Pool)

		return err
	})
	if response.IsNotFoundError(err) {
		// The storage pool doesn't exist. If backup is in binary format (so we cannot alter
		// the backup.yaml) or the pool has been specified directly from the user restoring
		// the backup then we cannot proceed so return an error.
		if *bInfo.OptimizedStorage || pool != "" {
			return response.InternalError(fmt.Errorf("Storage pool not found: %w", err))
		}

		var profile *api.Profile

		err = s.DB.Cluster.Transaction(context.TODO(), func(ctx context.Context, tx *db.ClusterTx) error {
			// Otherwise try and restore to the project's default profile pool.
			_, profile, err = tx.GetProfile(ctx, bInfo.Project, "default")

			return err
		})
		if err != nil {
			return response.InternalError(fmt.Errorf("Failed to get default profile: %w", err))
		}

		_, v, err := instancetype.GetRootDiskDevice(profile.Devices)
		if err != nil {
			return response.InternalError(fmt.Errorf("Failed to get root disk device: %w", err))
		}

		// Use the default-profile's root pool.
		bInfo.Pool = v["pool"]
	} else if err != nil {
		return response.InternalError(err)
	}

	// Copy reverter so far so we can use it inside run after this function has finished.
	runRevert := revert.Clone()

	run := func(_ *operations.Operation) error {
		defer func() { _ = backupFile.Close() }()
		defer runRevert.Fail()

		pool, err := storagePools.LoadByName(s, bInfo.Pool)
		if err != nil {
			return err
		}

		// Check if the backup is optimized that the source pool driver matches the target pool driver.
		if *bInfo.OptimizedStorage && pool.Driver().Info().Name != bInfo.Backend {
			return fmt.Errorf("Optimized backup storage driver %q differs from the target storage pool driver %q", bInfo.Backend, pool.Driver().Info().Name)
		}

		// Dump tarball to storage. Because the backup file is unpacked and restored onto the storage
		// device before the instance is created in the database it is necessary to return two functions;
		// a post hook that can be run once the instance has been created in the database to run any
		// storage layer finalisations, and a revert hook that can be run if the instance database load
		// process fails that will remove anything created thus far.
		postHook, revertHook, err := pool.CreateInstanceFromBackup(*bInfo, backupFile, nil)
		if err != nil {
			return fmt.Errorf("Create instance from backup: %w", err)
		}

		runRevert.Add(revertHook)

		err = internalImportFromBackup(context.TODO(), s, bInfo.Project, bInfo.Name, instanceName != "", devices)
		if err != nil {
			return fmt.Errorf("Failed importing backup: %w", err)
		}

		inst, err := instance.LoadByProjectAndName(s, bInfo.Project, bInfo.Name)
		if err != nil {
			return fmt.Errorf("Failed loading instance: %w", err)
		}

		// Clean up created instance if the post hook fails below.
		runRevert.Add(func() { _ = inst.Delete(true) })

		// Run the storage post hook to perform any final actions now that the instance has been created
		// in the database (this normally includes unmounting volumes that were mounted).
		if postHook != nil {
			err = postHook(inst)
			if err != nil {
				return fmt.Errorf("Post hook failed: %w", err)
			}
		}

		runRevert.Success()

		return instanceCreateFinish(s, &req, db.InstanceArgs{Name: bInfo.Name, Project: bInfo.Project})
	}

	resources := map[string][]api.URL{}
	resources["instances"] = []api.URL{*api.NewURL().Path(version.APIVersion, "instances", bInfo.Name)}

	op, err := operations.OperationCreate(r.Context(), s, bInfo.Project, operations.OperationClassTask, operationtype.BackupRestore, resources, nil, run, nil, nil)
	if err != nil {
		return response.InternalError(err)
	}

	revert.Success()
	return operations.OperationResponse(op)
}

// setupInstanceArgs sets the database instance arguments and determines the storage pool to use.
func setupInstanceArgs(s *state.State, instType instancetype.Type, projectName string, profiles []api.Profile, req *api.InstancesPost) (storagePool string, instArgs *db.InstanceArgs, resp response.Response) {
	// Parse the architecture name
	architecture, err := osarch.ArchitectureId(req.Architecture)
	if err != nil {
		return "", nil, response.BadRequest(err)
	}

	// Prepare the instance creation request.
	args := db.InstanceArgs{
		Project:      projectName,
		Architecture: architecture,
		BaseImage:    req.Source.BaseImage,
		Config:       req.Config,
		Type:         instType,
		Devices:      deviceConfig.NewDevices(req.Devices),
		Description:  req.Description,
		Ephemeral:    req.Ephemeral,
		Name:         req.Name,
		Profiles:     profiles,
		Stateful:     req.Stateful,
	}

	storagePool, storagePoolProfile, localRootDiskDeviceKey, localRootDiskDevice, resp := instanceFindStoragePool(s, projectName, req)
	if resp != nil {
		return "", nil, resp
	}

	if storagePool == "" {
		return "", nil, response.BadRequest(errors.New("Can't find a storage pool for the instance to use"))
	}

	if localRootDiskDeviceKey == "" && storagePoolProfile == "" {
		// Give the instance it's own local root disk device with a pool property.
		rootDev := map[string]string{}
		rootDev["type"] = "disk"
		rootDev["path"] = "/"
		rootDev["pool"] = storagePool
		if args.Devices == nil {
			args.Devices = deviceConfig.Devices{}
		}

		// Make sure that we do not overwrite a device the user is currently using
		// under the name "root".
		rootDevName := "root"
		for i := range 100 {
			if args.Devices[rootDevName] == nil {
				break
			}

			rootDevName = "root" + strconv.Itoa(i)
			continue
		}

		args.Devices[rootDevName] = rootDev
	} else if localRootDiskDeviceKey != "" && localRootDiskDevice["pool"] == "" {
		args.Devices[localRootDiskDeviceKey]["pool"] = storagePool
	}

	return storagePool, &args, nil
}

// swagger:operation POST /1.0/instances instances instances_post
//
//	Create a new instance
//
//	Creates a new instance on LXD.
//	Depending on the source, this can create an instance from an existing
//	local image, remote image, existing local instance or snapshot, remote
//	migration stream or backup file.
//
//	---
//	consumes:
//	  - application/json
//	  - application/octet-stream
//	produces:
//	  - application/json
//	parameters:
//	  - in: query
//	    name: project
//	    description: Project name
//	    type: string
//	    example: default
//	  - in: query
//	    name: target
//	    description: Cluster member
//	    type: string
//	    example: default
//	  - in: body
//	    name: instance
//	    description: Instance request
//	    required: false
//	    schema:
//	      $ref: "#/definitions/InstancesPost"
//	  - in: body
//	    name: raw_backup
//	    description: Raw backup file
//	    required: false
//	responses:
//	  "202":
//	    $ref: "#/responses/Operation"
//	  "400":
//	    $ref: "#/responses/BadRequest"
//	  "403":
//	    $ref: "#/responses/Forbidden"
//	  "500":
//	    $ref: "#/responses/InternalServerError"
func instancesPost(d *Daemon, r *http.Request) response.Response {
	s := d.State()

	targetProjectName := request.ProjectParam(r)
	clusterNotification := isClusterNotification(r)

	logger.Debug("Responding to instance create")

	// If we're getting binary content, process separately
	if r.Header.Get("Content-Type") == "application/octet-stream" {
		deviceMap := map[string]map[string]string{}

		if r.Header.Get("X-LXD-devices") != "" {
			devProps, err := url.ParseQuery(r.Header.Get("X-LXD-devices"))
			if err != nil {
				return response.BadRequest(err)
			}

			for devKey := range devProps {
				deviceMap[devKey] = map[string]string{}

				props, err := url.ParseQuery(devProps.Get(devKey))
				if err != nil {
					return response.BadRequest(err)
				}

				for k := range props {
					deviceMap[devKey][k] = props.Get(k)
				}
			}
		}

		return createFromBackup(s, r, targetProjectName, r.Body, r.Header.Get("X-LXD-pool"), r.Header.Get("X-LXD-name"), deviceMap)
	}

	// Parse the request
	req := api.InstancesPost{}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return response.BadRequest(err)
	}

	// Set type from URL if missing
	urlType, err := urlInstanceTypeDetect(r)
	if err != nil {
		return response.InternalError(err)
	}

	if req.Type == "" && urlType != instancetype.Any {
		req.Type = api.InstanceType(urlType.String())
	}

	if req.Type == "" {
		req.Type = api.InstanceTypeContainer // Default to container if not specified.
	}

	if req.Devices == nil {
		req.Devices = map[string]map[string]string{}
	}

	if req.Config == nil {
		req.Config = map[string]string{}
	}

	if req.InstanceType != "" {
		conf, err := instanceParseType(req.InstanceType)
		if err != nil {
			return response.BadRequest(err)
		}

		for k, v := range conf {
			if req.Config[k] == "" {
				req.Config[k] = v
			}
		}
	}

	var targetProject *api.Project
	var profiles []api.Profile
	var sourceInst *dbCluster.Instance
	var sourceImage *api.Image
	var sourceImageRef string
	var candidateMembers []db.NodeInfo
	var targetMemberInfo *db.NodeInfo

	err = s.DB.Cluster.Transaction(r.Context(), func(ctx context.Context, tx *db.ClusterTx) error {
		target := request.QueryParam(r, "target")
		if !s.ServerClustered && target != "" {
			return api.StatusErrorf(http.StatusBadRequest, "Target only allowed when clustered")
		}

		dbProject, err := dbCluster.GetProject(ctx, tx.Tx(), targetProjectName)
		if err != nil {
			return fmt.Errorf("Failed loading project: %w", err)
		}

		targetProject, err = dbProject.ToAPI(ctx, tx.Tx())
		if err != nil {
			return err
		}

		var targetGroupName string
		var allMembers []db.NodeInfo

		if s.ServerClustered && !clusterNotification {
			allMembers, err = tx.GetNodes(ctx)
			if err != nil {
				return fmt.Errorf("Failed getting cluster members: %w", err)
			}

			// Check if the given target is allowed and try to resolve the right member or group
			targetMemberInfo, targetGroupName, err = limits.CheckTarget(ctx, s.Authorizer, tx, targetProject, target, allMembers)
			if err != nil {
				return err
			}
		}

		profileProject := project.ProfileProjectFromRecord(targetProject)

		switch req.Source.Type {
		case api.SourceTypeCopy:
			if req.Source.Source == "" {
				return api.StatusErrorf(http.StatusBadRequest, "Must specify a source instance")
			}

			if req.Source.Project == "" {
				req.Source.Project = targetProjectName
			}

			sourceInst, err = instance.LoadInstanceDatabaseObject(ctx, tx, req.Source.Project, req.Source.Source)
			if err != nil {
				return err
			}

			req.Type = api.InstanceType(sourceInst.Type.String())

			// Use source instance's profiles if no profile override.
			if req.Profiles == nil {
				sourceInstArgs, err := tx.InstancesToInstanceArgs(ctx, true, *sourceInst)
				if err != nil {
					return err
				}

				req.Profiles = make([]string, 0, len(sourceInstArgs[sourceInst.ID].Profiles))
				for _, profile := range sourceInstArgs[sourceInst.ID].Profiles {
					req.Profiles = append(req.Profiles, profile.Name)
				}
			}

		case api.SourceTypeImage:
			// Check if the image has an entry in the database but fail only if the error
			// is different than the image not being found.
			sourceImage, err = getSourceImageFromInstanceSource(ctx, s, tx, targetProject.Name, req.Source, &sourceImageRef, string(req.Type))
			if err != nil && !api.StatusErrorCheck(err, http.StatusNotFound) {
				return err
			}

			// If image has an entry in the database then use its profiles if no override provided.
			if sourceImage != nil && req.Profiles == nil {
				req.Architecture = sourceImage.Architecture
				req.Profiles = sourceImage.Profiles
			}
		}

		// Use default profile if no profile list specified (not even an empty list).
		// This mirrors the logic in instance.CreateInternal() that would occur anyway.
		if req.Profiles == nil {
			req.Profiles = []string{"default"}
		}

		// Initialise the profile info list (even if an empty list is provided so this isn't left as nil).
		// This way instances can still be created without any profiles by providing a non-nil empty list.
		profiles = make([]api.Profile, 0, len(req.Profiles))

		// Load profiles.
		if len(req.Profiles) > 0 {
			profileFilters := make([]dbCluster.ProfileFilter, 0, len(req.Profiles))
			for _, profileName := range req.Profiles {
				profileFilters = append(profileFilters, dbCluster.ProfileFilter{
					Project: &profileProject,
					Name:    &profileName,
				})
			}

			dbProfiles, err := dbCluster.GetProfiles(ctx, tx.Tx(), profileFilters...)
			if err != nil {
				return err
			}

			dbProfileConfigs, err := dbCluster.GetConfig(ctx, tx.Tx(), "profile")
			if err != nil {
				return err
			}

			dbProfileDevices, err := dbCluster.GetDevices(ctx, tx.Tx(), "profile")
			if err != nil {
				return err
			}

			profilesByName := make(map[string]dbCluster.Profile, len(dbProfiles))
			for _, dbProfile := range dbProfiles {
				profilesByName[dbProfile.Name] = dbProfile
			}

			for _, profileName := range req.Profiles {
				profile, found := profilesByName[profileName]
				if !found {
					return fmt.Errorf("Requested profile %q doesn't exist", profileName)
				}

				apiProfile, err := profile.ToAPI(ctx, tx.Tx(), dbProfileConfigs, dbProfileDevices)
				if err != nil {
					return err
				}

				profiles = append(profiles, *apiProfile)
			}
		}

		// Generate automatic instance name if not specified.
		if req.Name == "" {
			names, err := tx.GetInstanceNames(ctx, targetProjectName)
			if err != nil {
				return err
			}

			i := 0
			for {
				i++
				req.Name = petname.Generate(2, "-")
				if !slices.Contains(names, req.Name) {
					break
				}

				if i > 100 {
					return errors.New("Couldn't generate a new unique name after 100 tries")
				}
			}

			logger.Debug("No name provided for new instance, using auto-generated name", logger.Ctx{"project": targetProjectName, "instance": req.Name})
		}

		if s.ServerClustered && !clusterNotification && targetMemberInfo == nil {
			architectures, err := instance.SuitableArchitectures(ctx, s, tx, targetProjectName, sourceInst, sourceImageRef, req)
			if err != nil {
				return err
			}

			// If no architectures have been ascertained from the source then use the default
			// architecture from project or global config if available.
			if len(architectures) < 1 {
				defaultArch := targetProject.Config["images.default_architecture"]
				if defaultArch == "" {
					defaultArch = s.GlobalConfig.ImagesDefaultArchitecture()
				}

				if defaultArch != "" {
					defaultArchID, err := osarch.ArchitectureId(defaultArch)
					if err != nil {
						return err
					}

					architectures = append(architectures, defaultArchID)
				} else {
					architectures = nil // Don't exclude candidate members based on architecture.
				}
			}

			clusterGroupsAllowed := limits.GetRestrictedClusterGroups(targetProject)

			candidateMembers, err = tx.GetCandidateMembers(ctx, allMembers, architectures, targetGroupName, clusterGroupsAllowed, s.GlobalConfig.OfflineThreshold())
			if err != nil {
				return err
			}
		}

		if !clusterNotification {
			// Check that the project's limits are not violated. Note this check is performed after
			// automatically generated config values (such as ones from an InstanceType) have been set.
			err = limits.AllowInstanceCreation(ctx, s.GlobalConfig, tx, targetProjectName, req)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return response.SmartError(err)
	}

	err = instancetype.ValidName(req.Name, false)
	if err != nil {
		return response.BadRequest(err)
	}

	if s.ServerClustered && !clusterNotification && targetMemberInfo == nil {
		// Run instance placement scriptlet if enabled and no cluster member selected yet.
		if s.GlobalConfig.InstancesPlacementScriptlet() != "" {
			leaderInfo, err := s.LeaderInfo()
			if err != nil {
				return response.InternalError(err)
			}

			// Copy request so we don't modify it when expanding the config.
			reqExpanded := apiScriptlet.InstancePlacement{
				InstancesPost: req,
				Project:       targetProjectName,
				Reason:        apiScriptlet.InstancePlacementReasonNew,
			}

			var globalConfigDump map[string]any
			if s.GlobalConfig != nil {
				globalConfigDump = s.GlobalConfig.Dump()
			}

			reqExpanded.Config = instancetype.ExpandInstanceConfig(globalConfigDump, reqExpanded.Config, profiles)
			reqExpanded.Devices = instancetype.ExpandInstanceDevices(deviceConfig.NewDevices(reqExpanded.Devices), profiles).CloneNative()

			targetMemberInfo, err = scriptlet.InstancePlacementRun(r.Context(), logger.Log, s, &reqExpanded, candidateMembers, leaderInfo.Address)
			if err != nil {
				return response.SmartError(fmt.Errorf("Failed instance placement scriptlet: %w", err))
			}
		}

		// If no target member was selected yet, pick the member with the least number of instances.
		if targetMemberInfo == nil {
			err = s.DB.Cluster.Transaction(r.Context(), func(ctx context.Context, tx *db.ClusterTx) error {
				targetMemberInfo, err = tx.GetNodeWithLeastInstances(ctx, candidateMembers)
				return err
			})
			if err != nil {
				return response.SmartError(err)
			}
		}
	}

	if targetMemberInfo != nil && targetMemberInfo.Address != "" && targetMemberInfo.Name != s.ServerName {
		client, err := cluster.Connect(r.Context(), targetMemberInfo.Address, s.Endpoints.NetworkCert(), s.ServerCert(), false)
		if err != nil {
			return response.SmartError(err)
		}

		client = client.UseProject(targetProjectName)
		client = client.UseTarget(targetMemberInfo.Name)

		logger.Debug("Forward instance post request", logger.Ctx{"local": s.ServerName, "target": targetMemberInfo.Name, "targetAddress": targetMemberInfo.Address})
		op, err := client.CreateInstance(req)
		if err != nil {
			return response.SmartError(err)
		}

		opAPI := op.Get()
		return operations.ForwardedOperationResponse(targetProjectName, &opAPI)
	}

	switch req.Source.Type {
	case api.SourceTypeImage:
		return createFromImage(r.Context(), s, *targetProject, profiles, sourceImage, sourceImageRef, &req)
	case api.SourceTypeNone:
		return createFromNone(r.Context(), s, targetProjectName, profiles, &req)
	case api.SourceTypeMigration:
		return createFromMigration(r.Context(), s, targetProjectName, profiles, &req, clusterNotification)
	case api.SourceTypeConversion:
		return createFromConversion(r.Context(), s, targetProjectName, profiles, &req)
	case api.SourceTypeCopy:
		return createFromCopy(r.Context(), s, targetProjectName, profiles, &req)
	default:
		return response.BadRequest(fmt.Errorf("Unknown source type %s", req.Source.Type))
	}
}

func instanceFindStoragePool(s *state.State, projectName string, req *api.InstancesPost) (storagePool string, storagePoolProfile string, localRootDiskDeviceKey string, localRootDiskDevice map[string]string, resp response.Response) {
	// Grab the container's root device if one is specified
	localRootDiskDeviceKey, localRootDiskDevice, _ = instancetype.GetRootDiskDevice(req.Devices)
	if localRootDiskDeviceKey != "" {
		storagePool = localRootDiskDevice["pool"]
	}

	// Handle copying/moving between two storage-api LXD instances.
	if storagePool != "" {
		err := s.DB.Cluster.Transaction(context.TODO(), func(ctx context.Context, tx *db.ClusterTx) error {
			_, err := tx.GetStoragePoolID(ctx, storagePool)

			return err
		})
		if response.IsNotFoundError(err) {
			storagePool = ""
			// Unset the local root disk device storage pool if not
			// found.
			localRootDiskDevice["pool"] = ""
		}
	}

	// If we don't have a valid pool yet, look through profiles
	if storagePool == "" {
		err := s.DB.Cluster.Transaction(context.TODO(), func(ctx context.Context, tx *db.ClusterTx) error {
			for _, pName := range req.Profiles {
				_, p, err := tx.GetProfile(ctx, projectName, pName)
				if err != nil {
					return err
				}

				k, v, _ := instancetype.GetRootDiskDevice(p.Devices)
				if k != "" && v["pool"] != "" {
					// Keep going as we want the last one in the profile chain
					storagePool = v["pool"]
					storagePoolProfile = pName
				}
			}

			return nil
		})
		if err != nil {
			return "", "", "", nil, response.SmartError(err)
		}
	}

	// If there is just a single pool in the database, use that
	if storagePool == "" {
		logger.Debug("No valid storage pool in the container's local root disk device and profiles found")

		var pools []string

		err := s.DB.Cluster.Transaction(context.TODO(), func(ctx context.Context, tx *db.ClusterTx) error {
			var err error

			pools, err = tx.GetStoragePoolNames(ctx)

			return err
		})
		if err != nil {
			if response.IsNotFoundError(err) {
				return "", "", "", nil, response.BadRequest(errors.New("This LXD instance does not have any storage pools configured"))
			}

			return "", "", "", nil, response.SmartError(err)
		}

		if len(pools) == 1 {
			storagePool = pools[0]
		}
	}

	return storagePool, storagePoolProfile, localRootDiskDeviceKey, localRootDiskDevice, nil
}

func clusterCopyContainerInternal(ctx context.Context, s *state.State, source instance.Instance, projectName string, profiles []api.Profile, req *api.InstancesPost) response.Response {
	// Locate the source of the container
	var nodeAddress string
	err := s.DB.Cluster.Transaction(ctx, func(ctx context.Context, tx *db.ClusterTx) error {
		var err error

		// Load source node.
		nodeAddress, err = tx.GetNodeAddressOfInstance(ctx, source.Project().Name, source.Name(), source.Type())
		if err != nil {
			return fmt.Errorf("Failed to get address of instance's member: %w", err)
		}

		return nil
	})
	if err != nil {
		return response.SmartError(err)
	}

	if nodeAddress == "" {
		return response.BadRequest(errors.New("The source instance is currently offline"))
	}

	// Connect to the container source
	client, err := cluster.Connect(ctx, nodeAddress, s.Endpoints.NetworkCert(), s.ServerCert(), false)
	if err != nil {
		return response.SmartError(err)
	}

	client = client.UseProject(source.Project().Name)

	// Setup websockets
	var opAPI api.Operation
	if shared.IsSnapshot(req.Source.Source) {
		cName, sName, _ := api.GetParentAndSnapshotName(req.Source.Source)

		pullReq := api.InstanceSnapshotPost{
			Migration: true,
			Live:      req.Source.Live,
			Name:      req.Name,
		}

		op, err := client.MigrateInstanceSnapshot(cName, sName, pullReq)
		if err != nil {
			return response.SmartError(err)
		}

		opAPI = op.Get()
	} else {
		// We keep the ContainerOnly for backward compatibility.
		instanceOnly := req.Source.InstanceOnly || req.Source.ContainerOnly //nolint:staticcheck,unused
		pullReq := api.InstancePost{
			Migration:     true,
			Live:          req.Source.Live,
			ContainerOnly: instanceOnly,
			InstanceOnly:  instanceOnly,
			Name:          req.Name,
		}

		op, err := client.MigrateInstance(req.Source.Source, pullReq)
		if err != nil {
			return response.SmartError(err)
		}

		opAPI = op.Get()
	}

	websockets := map[string]string{}
	for k, v := range opAPI.Metadata {
		ws, ok := v.(string)
		if !ok {
			continue
		}

		websockets[k] = ws
	}

	// Reset the source for a migration
	req.Source.Type = api.SourceTypeMigration
	req.Source.Certificate = string(s.Endpoints.NetworkCert().PublicKey())
	req.Source.Mode = "pull"
	req.Source.Operation = fmt.Sprintf("https://%s/%s/operations/%s", nodeAddress, version.APIVersion, opAPI.ID)
	req.Source.Websockets = websockets
	req.Source.Source = ""
	req.Source.Project = ""

	// Run the migration
	return createFromMigration(context.Background(), s, projectName, profiles, req, false)
}

// instanceCreateFinish finalizes the creation process of an instance by starting it based on
// the Start field of the request.
func instanceCreateFinish(s *state.State, req *api.InstancesPost, args db.InstanceArgs) error {
	if req == nil || !req.Start {
		return nil
	}

	// Start the instance.
	inst, err := instance.LoadByProjectAndName(s, args.Project, args.Name)
	if err != nil {
		return fmt.Errorf("Failed to load the instance: %w", err)
	}

	return inst.Start(false)
}
