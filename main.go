package main

import (
	"fmt"

	"github.com/raghavyuva/turbo-engine/internal/table"
)

func main() {
	firstTable := &table.Table{
		Name: "users",
		Columns: []table.Column{
			{Name: "id", DataType: table.Int}, {Name: "name", DataType: table.String, IsNullable: true},
			{Name: "email", DataType: table.String, IsNullable: true, IsUnique: true},
		},
	}

	anotherTable := &table.Table{
		Name: "products",
		Columns: []table.Column{
			{Name: "id", DataType: table.Int, IsPrimaryKey: true, IsNullable: true, IsAutoIncrement: true, IsUnique: false},
			{Name: "name", DataType: table.String, IsPrimaryKey: false, IsNullable: false, IsAutoIncrement: false, IsUnique: false},
			{Name: "price", DataType: table.Int, IsPrimaryKey: false, IsNullable: false, IsAutoIncrement: false, IsUnique: false, DefaultValue: "0"},
		},
	}

	metadata := table.NewMetaData()
	diskManager := table.NewDiskManager()
	transactionLog := table.NewTransactionLog()
	page := table.NewPage()

	err := firstTable.CreateTable(firstTable.Name, firstTable.Columns, metadata, diskManager, transactionLog, page)
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
