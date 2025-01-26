package internal

import (
	"errors"
	"fmt"
)

var (
	errTableDoesNotExist   = errors.New("table does not exist")
	errColumnAlreadyExists = errors.New("column already exists")
	errColumnDoesNotExist  = errors.New("column does not exist")
)

func (t *Table) AlterTable() error {
	if t.ValidateTable(NewMetaData(), NewDiskManager()) != nil {
		fmt.Printf("Error validating while altering table: %v\n", t.ValidateTable(NewMetaData(), NewDiskManager()))
		return t.ValidateTable(NewMetaData(), NewDiskManager())
	}

	return nil
}

// ValidateAlterTable validates the alter table operation. It checks if the table name
// is valid and if the columns to be added, renamed, or updated are valid.
// It returns an error if the table name is empty, too short, or too long,
// or if any of the columns to be added, renamed, or updated has an empty or
// invalid name, or if any of the columns to be updated has an invalid data type.
func (t *AlterTable) ValidateAlterTable(m *MetaData, d *DiskManager) error {
	if t.Name == "" {
		return errTableNameEmpty
	}
	if len(t.Name) < 2 {
		return errTableNameTooShort
	}
	if len(t.Name) > 64 {
		return errTableNameTooLong
	}

	exists := m.isTableExists(t.Name, d)
	if !exists {
		return errTableDoesNotExist
	}

	for _, column := range t.AddedColumns {
		if column.Name == "" {
			return errInvalidColumnName
		}
		if len(column.Name) < 3 {
			return errColumnNameTooShort
		}
		if len(column.Name) > 64 {
			return errColumnNameTooLong
		}

		if column.DataType != Int && column.DataType != String && column.DataType != Bool {
			return errInvalidDataType
		}

		if m.isColumnExists(column.Name, d) {
			return errColumnAlreadyExists
		}
	}

	for _, column := range t.RenamedColumns {
		if column.OldName == "" {
			return errInvalidColumnName
		}

		if column.NewName == "" {
			return errInvalidColumnName
		}

		if len(column.NewName) < 3 {
			return errColumnNameTooShort
		}
		if len(column.NewName) > 64 {
			return errColumnNameTooLong
		}

		if column.OldName == column.NewName {
			return errDuplicateColumn
		}

		if !m.isColumnExists(column.OldName, d) {
			return errColumnDoesNotExist
		}

		if m.isColumnExists(column.NewName, d) {
			return errColumnAlreadyExists
		}
	}

	for _, column := range t.UpdatedColumnDataTypes {
		if column.DataType == Int || column.DataType == String || column.DataType == Bool {
			continue
		}
		return errInvalidDataType
	}

	return nil
}
