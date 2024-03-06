package cluster

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/canonical/lxd/lxd/db/query"
	"github.com/canonical/lxd/shared/api"
)

// Code generation directives.
//
//go:generate -command mapper lxd-generate db mapper -t identity_provider_groups.mapper.go
//go:generate mapper reset -i -b "//go:build linux && cgo && !agent"
//
//go:generate mapper stmt -e identity_provider_group objects table=identity_provider_groups
//go:generate mapper stmt -e identity_provider_group objects-by-ID table=identity_provider_groups
//go:generate mapper stmt -e identity_provider_group objects-by-Name table=identity_provider_groups
//go:generate mapper stmt -e identity_provider_group id table=identity_provider_groups
//go:generate mapper stmt -e identity_provider_group create table=identity_provider_groups
//go:generate mapper stmt -e identity_provider_group delete-by-Name table=identity_provider_groups
//go:generate mapper stmt -e identity_provider_group update table=identity_provider_groups
//go:generate mapper stmt -e identity_provider_group rename table=identity_provider_groups
//
//go:generate mapper method -i -e identity_provider_group GetMany
//go:generate mapper method -i -e identity_provider_group GetOne
//go:generate mapper method -i -e identity_provider_group ID
//go:generate mapper method -i -e identity_provider_group Exists
//go:generate mapper method -i -e identity_provider_group Create
//go:generate mapper method -i -e identity_provider_group DeleteOne-by-Name
//go:generate mapper method -i -e identity_provider_group Update
//go:generate mapper method -i -e identity_provider_group Rename

// IdentityProviderGroup is the database representation of an api.IdentityProviderGroup.
type IdentityProviderGroup struct {
	ID   int
	Name string `db:"primary=true"`
}

// IdentityProviderGroupFilter contains the columns that a queries for identity provider groups can be filtered upon.
type IdentityProviderGroupFilter struct {
	ID   *int
	Name *string
}

// ToAPI converts the IdentityProviderGroup to an api.IdentityProviderGroup, making more database calls as necessary.
func (i *IdentityProviderGroup) ToAPI(ctx context.Context, tx *sql.Tx) (*api.IdentityProviderGroup, error) {
	idpGroup := &api.IdentityProviderGroup{
		Name: i.Name,
	}

	groups, err := GetAuthGroupsByIdentityProviderGroupID(ctx, tx, i.ID)
	if err != nil {
		return nil, err
	}

	groupNames := make([]string, 0, len(groups))
	for _, group := range groups {
		groupNames = append(groupNames, group.Name)
	}

	idpGroup.Groups = groupNames
	return idpGroup, nil
}

// GetAuthGroupsByIdentityProviderGroupID returns a list of a groups that the identity provider group with the given ID.
func GetAuthGroupsByIdentityProviderGroupID(ctx context.Context, tx *sql.Tx, idpGroupID int) ([]AuthGroup, error) {
	stmt := `
SELECT auth_groups.id, auth_groups.name, auth_groups.description
FROM auth_groups_identity_provider_groups
JOIN auth_groups ON auth_groups_identity_provider_groups.auth_group_id = auth_groups.id
WHERE auth_groups_identity_provider_groups.identity_provider_group_id = ?`

	var result []AuthGroup
	dest := func(scan func(dest ...any) error) error {
		g := AuthGroup{}
		err := scan(&g.ID, &g.Name, &g.Description)
		if err != nil {
			return err
		}

		result = append(result, g)

		return nil
	}

	err := query.Scan(ctx, tx, stmt, dest, idpGroupID)
	if err != nil {
		return nil, fmt.Errorf("Failed to get group mappings for identity provider group with ID `%d`: %w", idpGroupID, err)
	}

	return result, nil
}

// SetIdentityProviderGroupMapping deletes all auth_group -> identity_provider_group mappings from the `ath_groups_identity_provider_groups` table
// where the identity provider group ID is equal to the given value. Then it inserts new assocations into the table where the
// group IDs correspond to the given group names.
func SetIdentityProviderGroupMapping(ctx context.Context, tx *sql.Tx, identityProviderGroupID int, groupNames []string) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM auth_groups_identity_provider_groups WHERE identity_provider_group_id = ?`, identityProviderGroupID)
	if err != nil {
		return fmt.Errorf("Failed to delete existing identity provider group mappings: %w", err)
	}

	if len(groupNames) == 0 {
		return nil
	}

	args := []any{identityProviderGroupID}
	var builder strings.Builder
	builder.WriteString(`
INSERT INTO auth_groups_identity_provider_groups (auth_group_id, identity_provider_group_id)
SELECT ?, auth_groups.id
FROM auth_groups
WHERE auth_groups.name IN (
`)
	for i, groupName := range groupNames {
		if i == len(groupNames)-1 {
			builder.WriteString(`?)`)
		} else {
			builder.WriteString(`?, `)
		}

		args = append(args, groupName)
	}

	res, err := tx.ExecContext(ctx, builder.String(), args...)
	if err != nil {
		return fmt.Errorf("Failed to write identity provider group mappings: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("Failed to check validity of identity provider group mapping creation: %w", err)
	}

	if int(rowsAffected) != len(groupNames) {
		return fmt.Errorf("Failed to write expected number of rows to identity provider group association table (expected %d, got %d)", len(groupNames), rowsAffected)
	}

	return nil
}
