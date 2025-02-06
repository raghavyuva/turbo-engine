package table

import (
	"errors"
	"fmt"
)

var (
	errTableDoesNotExist   = errors.New("table does not exist")
	errColumnAlreadyExists = errors.New("column already exists")
	errColumnDoesNotExist  = errors.New("column does not exist")
)

func (t *AlterTable) AlterTable(Name string, DeletedColumns []string, AddedColumns []Column, RenamedColumns []RenamedColumns, UpdatedColumnDataTypes []Column, d *DiskManager, m *TableMetadata) error {

	setAlterTable(t, Name, DeletedColumns, AddedColumns, RenamedColumns, UpdatedColumnDataTypes)

	err := t.ValidateAlterTable(m, d)
	if err != nil {
		fmt.Printf("Error validating while altering table: %v\n", err)
		return err
	}

	return nil
}

// setAlterTable sets the values for the AlterTable struct.
// It takes the table name, columns to be deleted, columns to be added,
// columns to be renamed, and columns to have their data type updated
// as parameters and sets the corresponding fields in the AlterTable struct.
func setAlterTable(t *AlterTable, Name string, DeletedColumns []string, AddedColumns []Column, RenamedColumns []RenamedColumns, UpdatedColumnDataTypes []Column) {
	t.SetName(Name)
	t.SetDeletedColumns(DeletedColumns)
	t.SetAddedColumns(AddedColumns)
	t.SetRenamedColumns(RenamedColumns)
	t.SetUpdatedColumnDataTypes(UpdatedColumnDataTypes)
}

// ValidateAlterTable validates the alter table operation. It checks if the table name
// is valid and if the columns to be added, renamed, or updated are valid.
// It returns an error if the table name is empty, too short, or too long,
// or if any of the columns to be added, renamed, or updated has an empty or
// invalid name, or if any of the columns to be updated has an invalid data type.
func (t *AlterTable) ValidateAlterTable(m *TableMetadata, d *DiskManager) error {
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

// SetName sets the name of the table being altered.
func (t *AlterTable) SetName(name string) {
	t.Name = name
}

// SetDeletedColumns sets the columns that are to be deleted from the table.
func (t *AlterTable) SetDeletedColumns(columns []string) {
	t.DeletedColumns = columns
}

// SetAddedColumns sets the columns that are to be added to the table being altered.
func (t *AlterTable) SetAddedColumns(columns []Column) {
	t.AddedColumns = columns
}

// SetRenamedColumns sets the columns whose names are to be changed in the table
// being altered.
func (t *AlterTable) SetRenamedColumns(columns []RenamedColumns) {
	t.RenamedColumns = columns
}

// SetUpdatedColumnDataTypes sets the columns whose data types are to be updated in the table
// being altered.
func (t *AlterTable) SetUpdatedColumnDataTypes(columns []Column) {
	t.UpdatedColumnDataTypes = columns
}

// DeleteColumns removes the specified columns from the metadata of the table.
// It reads the current metadata, removes the columns listed in DeletedColumns,
// and writes the updated metadata back to the disk. It returns an error if
// reading or writing the metadata fails.
func (at *AlterTable) DeleteColumns(m *TableMetadata, d *DiskManager, t *Table) error {
	metadata, err := m.ReadTableMetaData(d)
	if err != nil {
		return err
	}

	for _, column := range at.DeletedColumns {
		for i, table := range metadata {
			if table["Name"] == at.Name {
				columns := table["Columns"].([]string)
				for j, col := range columns {
					if col == column {
						columns = append(columns[:j], columns[j+1:]...)
						metadata[i]["Columns"] = columns
					}
				}
			}
		}
	}
	err = m.WriteCreateTableMetaData(t, d)
	if err != nil {
		return err
	}
	return nil
}

func (t *AlterTable) AddColumns() {

}

func (t *AlterTable) RenameColumns() {

}

func (t *AlterTable) UpdateColumnDataTypes() {

}

func (t *AlterTable) UpdateMetaData() {

}

func (t *AlterTable) UpdatePageDirectory() {

}

func (t *AlterTable) UpdateTransactionLog() {

}
