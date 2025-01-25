package main

import (
	"fmt"
	"github.com/raghavyuva/turbo-engine/internal"
)

func main() {
	table := &internal.Table{
		Name:    "users",
		Columns: []internal.Column{{Name: "id", DataType: internal.Int}, {Name: "name", DataType: internal.String}},
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

	fmt.Printf("Table created successfully\n")
}
