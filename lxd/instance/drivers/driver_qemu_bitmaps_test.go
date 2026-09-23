package drivers

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/canonical/lxd/shared"
)

// TestSnapshotBitmapFile checks that a snapshot bitmap file is read back as written when the instance snapshot UUID
// it records is asked for, and that no file is returned for another UUID or from a directory without one.
func TestSnapshotBitmapFile(t *testing.T) {
	dir := t.TempDir()
	file := &snapshotBitmapFile{
		Snapshot: snapshotBitmapFileSnapshot{UUID: "7d3c9a1e-4b2f-4e8a-9c1d-2a6f8b3e5d70", Name: "snap3"},
		Volumes: map[string]snapshotBitmapFileVolume{
			"root": {
				UUID: "891bd2a3-7d4e-4c5a-9b1f-0e2d3c4b5a6f",
				Bitmaps: []snapshotBitmapFileBitmap{
					{Name: "snap1", UUID: "2f0a6b77-1c3d-4e5f-8a9b-0c1d2e3f4a5b", Granularity: 65536},
					{Name: "snap2", UUID: "5e114c02-9d8e-4f7a-b6c5-d4e3f2a1b0c9", Granularity: 65536},
				},
			},
			"data": {
				UUID:    "c4d2e8f0-3a5b-4c7d-9e1f-2a3b4c5d6e7f",
				Bitmaps: []snapshotBitmapFileBitmap{{Name: "snap2", UUID: "5e114c02-9d8e-4f7a-b6c5-d4e3f2a1b0c9", Granularity: 65536}},
			},
		},
	}

	require.NoError(t, writeSnapshotBitmapFile(dir, file))
	require.True(t, shared.PathExists(filepath.Join(dir, "snapshot.snap3.yaml")), "The file is named after the snapshot")

	read, err := readSnapshotBitmapFile(dir, file.Snapshot.UUID)
	require.NoError(t, err)
	require.Equal(t, file, read)

	read, err = readSnapshotBitmapFile(dir, "00000000-0000-0000-0000-000000000000")
	require.NoError(t, err)
	require.Nil(t, read, "A file recording another snapshot is not returned")

	read, err = readSnapshotBitmapFile(filepath.Join(dir, "missing"), file.Snapshot.UUID)
	require.NoError(t, err)
	require.Nil(t, read, "A missing directory holds no file")
}
