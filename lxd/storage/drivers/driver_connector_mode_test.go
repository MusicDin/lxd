package drivers

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/canonical/lxd/lxd/storage/connectors"
)

// requireRuleAllowed fails the test if error signals a rejection by driver-level
// validation rules or the mode-change check.
//
// Errors from kernel-module loading are tolerated because the test environment
// may not have NVMe/iSCSI/SDC modules available, and that check runs after connector type validation.
func requireRuleAllowed(t *testing.T, err error) {
	t.Helper()

	if err == nil {
		return
	}

	if strings.Contains(err.Error(), "kernel modules") {
		return
	}

	t.Fatalf("Expected validation to pass, got: %v", err)
}

// connectorDriver describes a storage driver that uses a connector type as part of its pool config.
type connectorDriver struct {
	// Name is the human-readable driver name.
	Name string

	// ModeKey is the pool config key holding the connector mode.
	ModeKey string

	// SupportedModes is the set of connector modes supported by the driver.
	SupportedModes []string

	// SkipInit forces the driver's package-level loaded flag to true and
	// registers a cleanup to restore it. This skips per-process init in load(),
	// so the test only exercises the mode normalization step.
	SkipInit func(t *testing.T)

	// NewDriver returns a fresh, uninitialised driver of the appropriate type.
	NewDriver func() driver
}

// connectorDrivers returns the set of storage drivers that use connectors package.
func connectorDrivers() []connectorDriver {
	return []connectorDriver{
		{
			Name:           "PowerFlex",
			ModeKey:        "powerflex.mode",
			SupportedModes: powerflexSupportedConnectors,
			SkipInit: func(t *testing.T) {
				saved := powerFlexLoaded
				powerFlexLoaded = true
				t.Cleanup(func() { powerFlexLoaded = saved })
			},
			NewDriver: func() driver { return &powerflex{} },
		},
		{
			Name:           "Pure Storage",
			ModeKey:        "pure.mode",
			SupportedModes: pureSupportedConnectors,
			SkipInit: func(t *testing.T) {
				saved := pureLoaded
				pureLoaded = true
				t.Cleanup(func() { pureLoaded = saved })
			},
			NewDriver: func() driver { return &pure{} },
		},
		{
			Name:           "Alletra Storage",
			ModeKey:        "alletra.mode",
			SupportedModes: alletraSupportedConnectors,
			SkipInit: func(t *testing.T) {
				saved := alletraLoaded
				alletraLoaded = true
				t.Cleanup(func() { alletraLoaded = saved })
			},
			NewDriver: func() driver { return &alletra{} },
		},
		{
			Name:           "PowerStore",
			ModeKey:        "powerstore.mode",
			SupportedModes: powerStoreSupportedConnectors,
			SkipInit: func(t *testing.T) {
				saved := powerStoreLoaded
				powerStoreLoaded = true
				t.Cleanup(func() { powerStoreLoaded = saved })
			},
			NewDriver: func() driver { return &powerstore{} },
		},
	}
}

// initDriver constructs a driver, with d.config populated from the given mode value (omitted
// entirely when empty) and d.commonRules set to a minimal validator.
func initDriver(cd connectorDriver, currentMode string) driver {
	config := map[string]string{}
	if currentMode != "" {
		config[cd.ModeKey] = currentMode
	}

	emptyValidators := &Validators{
		PoolRules: func() map[string]func(string) error {
			return map[string]func(string) error{}
		},
		VolumeRules: func(_ Volume) map[string]func(string) error {
			return map[string]func(string) error{}
		},
	}

	d := cd.NewDriver()
	d.init(nil, "", config, nil, nil, emptyValidators)
	return d
}

func Test_storageDriver_load_normalizesLegacyMode(t *testing.T) {
	type driverLoadTest struct {
		Name        string
		CurrentMode string
		Expect      string
	}

	for _, cd := range connectorDrivers() {
		t.Run(cd.Name, func(t *testing.T) {
			cd.SkipInit(t)
			supportsNVMeTCP := slices.Contains(cd.SupportedModes, connectors.TypeNVMeTCP)

			tests := []driverLoadTest{{
				Name:        "Empty mode is preserved",
				CurrentMode: "",
				Expect:      "",
			}}

			if supportsNVMeTCP {
				tests = append(tests, driverLoadTest{
					Name:        "Legacy NVMe mode is normalized to new NVMe/TCP mode",
					CurrentMode: "nvme",
					Expect:      "nvme/tcp",
				})
			}

			// Each supported mode is left untouched.
			for _, mode := range cd.SupportedModes {
				tests = append(tests, driverLoadTest{
					Name:        fmt.Sprintf("Canonical %s mode is preserved", mode),
					CurrentMode: mode,
					Expect:      mode,
				})
			}

			for _, test := range tests {
				t.Run(test.Name, func(t *testing.T) {
					d := initDriver(cd, test.CurrentMode)
					require.NoError(t, d.load())
					assert.Equal(t, test.Expect, d.Config()[cd.ModeKey])
				})
			}
		})
	}
}

func Test_storageDriver_Validate_mode(t *testing.T) {
	type driverValidateTest struct {
		Name            string
		CurrentMode     string // Value already in d.config (simulates an existing pool).
		NewMode         string // Value submitted in the new config.
		ExpectErrSubstr string // Empty means expect no error (subject to kernel-modules tolerance).
	}

	for _, cd := range connectorDrivers() {
		t.Run(cd.Name, func(t *testing.T) {
			invalidValueErr := fmt.Sprintf("Invalid value for option %q", cd.ModeKey)
			cannotChangeErr := cd.Name + " mode cannot be changed"
			supportsNVMeTCP := slices.Contains(cd.SupportedModes, connectors.TypeNVMeTCP)

			tests := []driverValidateTest{
				{
					Name:            "New pool rejects unknown mode",
					NewMode:         "garbage",
					ExpectErrSubstr: invalidValueErr,
				},
			}

			if supportsNVMeTCP {
				tests = append(tests,
					driverValidateTest{
						Name:    "New pool accepts legacy NVMe alias",
						NewMode: "nvme",
					},
					driverValidateTest{
						Name:        "Legacy NVMe alias with same current mode is allowed",
						CurrentMode: "nvme",
						NewMode:     "nvme",
					},
					driverValidateTest{
						Name:        "Renaming legacy NVMe mode to new NVMe/TCP mode is allowed",
						CurrentMode: "nvme",
						NewMode:     "nvme/tcp",
					},
				)
			}

			// Each supported mode is accepted on a new pool.
			for _, mode := range cd.SupportedModes {
				tests = append(tests,
					driverValidateTest{
						Name:    "New pool accepts connector mode " + mode,
						NewMode: mode,
					},
					driverValidateTest{
						Name:        "Same connector mode " + mode + " is allowed",
						CurrentMode: mode,
						NewMode:     mode,
					},
				)
			}

			// Current connector mode cannot be changed to any other mode.
			for _, from := range cd.SupportedModes {
				for _, to := range cd.SupportedModes {
					if from == to {
						continue
					}

					tests = append(tests, driverValidateTest{
						Name:            "Connector mode " + from + " cannot be changed to " + to,
						CurrentMode:     from,
						NewMode:         to,
						ExpectErrSubstr: cannotChangeErr,
					})
				}
			}

			for _, test := range tests {
				t.Run(test.Name, func(t *testing.T) {
					d := initDriver(cd, test.CurrentMode)

					err := d.Validate(map[string]string{cd.ModeKey: test.NewMode})

					if test.ExpectErrSubstr != "" {
						require.Error(t, err)
						assert.Contains(t, err.Error(), test.ExpectErrSubstr)
					} else {
						requireRuleAllowed(t, err)
					}
				})
			}
		})
	}
}
