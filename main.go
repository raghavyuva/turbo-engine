package main

import (
	"fmt"
	"github.com/raghavyuva/turbo-engine/internal/table"
)

func main() {
	table := &internal.Table{
		Name: "users",
		Columns: []internal.Column{
			{Name: "id", DataType: internal.Int}, {Name: "name", DataType: internal.String, IsNullable: true},
			{Name: "email", DataType: internal.String, IsNullable: true,IsUnique: true},
		},
	}

	anotherTable := &internal.Table{
		Name: "products",
		Columns: []internal.Column{
			{Name: "id", DataType: internal.Int, IsPrimaryKey: true, IsNullable: true, IsAutoIncrement: true, IsUnique: false},
			{Name: "name", DataType: internal.String, IsPrimaryKey: false, IsNullable: false, IsAutoIncrement: false, IsUnique: false},
			{Name: "price", DataType: internal.Int, IsPrimaryKey: false, IsNullable: false, IsAutoIncrement: false, IsUnique: false, DefaultValue: "0"},
		},
	}
	metadata := internal.NewMetaData()
	diskManager := internal.NewDiskManager()
	transactionLog := internal.NewTransactionLog()
	page := internal.NewPage()

	err := table.CreateTable(table.Name, table.Columns, metadata, diskManager, transactionLog, page)
	if err != nil {
		fmt.Printf("Error creating table: %v\n", err)
		return
	}
	err = anotherTable.CreateTable(anotherTable.Name, anotherTable.Columns, metadata, diskManager, transactionLog, page)
	if err != nil {
		fmt.Printf("Error creating table: %v\n", err)
		return
	}
	fmt.Printf("Table created successfully\n")
}
