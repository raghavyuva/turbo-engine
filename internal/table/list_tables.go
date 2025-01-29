package internal

// ListAllTables retrieves all tables' metadata from the disk and returns them
// as a slice of Table objects. It reads the metadata using the provided
// DiskManager, constructs Table objects with the relevant information, and
// appends them to the slice. Returns an error if reading the metadata fails.
func (m *TableMetadata) ListAllTables(d *DiskManager) ([]*Table, error) {
	var tables []*Table
	metadata, err := m.ReadTableMetaData(d)
	if err != nil {
		return nil, err
	}
	for _, table := range metadata {
		t := NewTable(table["Name"].(string), nil, nil, uint64(table["Size"].(int)), uint64(table["RowsCount"].(int)))
		tables = append(tables, t)
	}
	return tables, nil
}
