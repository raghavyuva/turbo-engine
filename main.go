package main

import (
	"fmt"
	"github.com/raghavyuva/turbo-engine/internal/table"
)

func main() {
	table := &internal.Table{
		Name:    "users",
		Columns: []internal.Column{{Name: "id", DataType: internal.Int}, {Name: "name", DataType: internal.String}},
	}

	anotherTable := &internal.Table{
		Name: "products",
		Columns: []internal.Column{
			{Name: "id", DataType: internal.Int},
			{Name: "name", DataType: internal.String},
			{Name: "price", DataType: internal.Int},
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
