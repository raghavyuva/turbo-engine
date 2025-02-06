package table

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ListAllTables(t *testing.T) {
	os.Mkdir("database", 0755)
	defer os.RemoveAll("database")

	table := &Table{
		Name: "users",
		Columns: []Column{
			{Name: "id", DataType: Int}, {Name: "name", DataType: String, IsNullable: true},
			{Name: "email", DataType: String, IsNullable: true, IsUnique: true},
		},
	}

	anotherTable := &Table{
		Name: "products",
		Columns: []Column{
			{Name: "id", DataType: Int, IsPrimaryKey: true, IsNullable: true, IsAutoIncrement: true, IsUnique: false},
			{Name: "name", DataType: String, IsPrimaryKey: false, IsNullable: false, IsAutoIncrement: false, IsUnique: false},
			{Name: "price", DataType: Int, IsPrimaryKey: false, IsNullable: false, IsAutoIncrement: false, IsUnique: false, DefaultValue: "0"},
		},
	}
	metadata := NewMetaData()
	diskManager := NewDiskManager()
	transactionLog := NewTransactionLog()
	page := NewPage()

	err := table.CreateTable(table.Name, table.Columns, metadata, diskManager, transactionLog, page)
	if err != nil {
		t.Errorf("Error creating table: %v", err)
		return
	}
	err = anotherTable.CreateTable(anotherTable.Name, anotherTable.Columns, metadata, diskManager, transactionLog, page)
	if err != nil {
		t.Errorf("Error creating table: %v", err)
		return
	}

	tables, err := metadata.ListAllTables(diskManager)

	if err != nil {
		t.Errorf("Error listing tables: %v", err)
		return
	}

	tableNames := make([]string, len(tables))
	for i, table := range tables {
		tableNames[i] = table.Name
	}

	assert.ElementsMatch(t, tableNames, []string{"users", "products"})
}
