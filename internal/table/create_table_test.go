package internal

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Test_CreateTable tests the CreateTable method of the Table struct
func Test_CreateTable(t *testing.T) {
	err := os.MkdirAll("./database", 0755)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./database")
	table := &Table{
		Name:    "users",
		Columns: []Column{{Name: "id", DataType: Int}, {Name: "name", DataType: String}},
	}

	metadata := NewMetaData()
	diskManager := NewDiskManager()
	transactionLog := NewTransactionLog()
	page := NewPage()

	err = table.CreateTable(table.Name, table.Columns, metadata, diskManager, transactionLog, page)

	if err != nil {
		t.Errorf("Error creating table: %v", err)
		return
	}

	assert.Nil(t, err)
}

// Test_ValidateTable tests the ValidateTable method of the Table struct
func Test_ValidateTable(t *testing.T) {
	metadata := NewMetaData()
	diskManager := NewDiskManager()

	if err := os.MkdirAll("./database", 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./database")

	t.Run("duplicate table check", func(t *testing.T) {
		table := &Table{
			Name:    "users",
			Columns: []Column{{Name: "id", DataType: Int}, {Name: "name", DataType: String}},
		}

		err := table.ValidateTable(metadata, diskManager)
		assert.Nil(t, err)

		err = table.ValidateTable(metadata, diskManager)
		assert.Nil(t, err)
	})

	t.Run("duplicate column check", func(t *testing.T) {
		table := &Table{
			Name: "basket",
			Columns: []Column{
				{Name: "id", DataType: Int},
				{Name: "name", DataType: String},
				{Name: "name", DataType: String},
			},
		}

		err := table.ValidateTable(metadata, diskManager)
		assert.Equal(t, errDuplicateColumn, err)
	})

	t.Run("invalid column name", func(t *testing.T) {
		table := &Table{
			Name: "items",
			Columns: []Column{
				{Name: "", DataType: Int},
			},
		}

		err := table.ValidateTable(metadata, diskManager)
		assert.Equal(t, errInvalidColumnName, err)
	})

	t.Run("invalid table name", func(t *testing.T) {
		table := &Table{
			Name:    "",
			Columns: []Column{{Name: "id", DataType: Int}},
		}

		err := table.ValidateTable(metadata, diskManager)
		assert.Equal(t, errTableNameEmpty, err)
	})
}

// Test_allocateResources tests the allocateResources method of the Table struct.
// It tests the successful path, and asserts that the error is nil.
func Test_allocateResources(t *testing.T) {
	err := os.MkdirAll("database", 0755)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("database")
	table := &Table{
		Name:    "users",
		Columns: []Column{{Name: "id", DataType: Int}, {Name: "name", DataType: String}},
	}

	disk_manager := NewDiskManager()
	page := NewPage()

	err = table.allocateResources(disk_manager, page)

	assert.Nil(t, err)
}

// Test_WritePage tests the WritePage method of the DiskManager struct.
// It includes two sub-tests:
//  1. "No Database directory exists" checks that an error is returned when trying to write a page
//     without a valid database directory, expecting the errSeekingFile error.
//  2. "Database directory exists" verifies that when a valid database directory is present,
//     the page is written successfully without errors.
func Test_WritePage(t *testing.T) {
	t.Run("No Database directory exists", func(t *testing.T) {
		disk_manager := NewDiskManager()
		page := NewPage()

		err := disk_manager.WritePage(*page)

		assert.Equal(t, err, errSeekingFile)
	})

	t.Run("Database directory exists", func(t *testing.T) {
		err := os.MkdirAll("database", 0755)
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll("database")

		disk_manager := NewDiskManager()
		page := NewPage()

		err = disk_manager.WritePage(*page)

		assert.Nil(t, err)
	})
}

// Test_WriteMetaData tests the WriteMetaData method of the MetaData struct.
// It includes a sub-test:
//  1. "No Database directory exists" checks that an error is returned when trying to write
//     metadata without a valid database directory, expecting the errSeekingFile error.
func Test_WriteMetaData(t *testing.T) {
	disk_manager := NewDiskManager()
	metadata := NewMetaData()

	table := &Table{
		Name:    "users",
		Columns: []Column{{Name: "id", DataType: Int}, {Name: "name", DataType: String}},
	}

	err := metadata.WriteCreateTableMetaData(table, disk_manager)

	assert.Equal(t, err, errSeekingFile)
}

// Test_WriteCreateTableLog tests the WriteCreateTableLog method of the TransactionLog struct.
// It includes two sub-tests:
//  1. "No file exists" checks that an error is returned when trying to write a create table log
//     to a non-existent file, expecting the errWritingLog error.
//  2. "file exists" checks that writing a create table log to an existing file succeeds.
func Test_WriteCreateTableLog(t *testing.T) {
	t.Run("No file exists", func(t *testing.T) {
		disk_manager := NewDiskManager()
		table := &Table{
			Name:    "users",
			Columns: []Column{{Name: "id", DataType: Int}, {Name: "name", DataType: String}},
		}

		log := NewTransactionLog()

		err := log.WriteCreateTableLog(table, disk_manager)

		assert.Equal(t, err, errWritingLog)
	})

	t.Run("file exists", func(t *testing.T) {
		err := os.MkdirAll("database", 0755)
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll("database")

		disk_manager := NewDiskManager()
		table := &Table{
			Name:    "users",
			Columns: []Column{{Name: "id", DataType: Int}, {Name: "name", DataType: String}},
		}
		log := NewTransactionLog()

		err = log.WriteCreateTableLog(table, disk_manager)

		assert.Nil(t, err)
	})
}
