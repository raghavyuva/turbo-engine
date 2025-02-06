package table

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io"
	"math/rand"
	"time"
)

// ------- Table Setters --------

// Name Setter method for Table struct
func (t *Table) SetName(name string) {
	t.Name = name
}

// Columns Setter method for Table struct
func (t *Table) SetColumns(columns []Column) {
	t.Columns = columns
}

// Size Setter method for Table struct
func (t *Table) SetSize(size uint64) {
	t.Size = size
}

// RowsCount Setter method for Table struct
func (t *Table) SetRowsCount(rowsCount uint64) {
	t.RowsCount = rowsCount
}

// GenerateID generates a unique ID for the table
func (t *Table) GenerateID() uuid.UUID {
	return uuid.New()
}

// Errors for the table creation

var (
	errWritingMetaData = errors.New("error writing metadata")
	errWritingLog      = errors.New("error writing log")
	errEncodingLog     = errors.New("error while encoding the log")
)

// Creating a table

// CreateTable creates a table with the given name and columns, and writes the
// corresponding metadata and log entries. If the name is empty, the table's
// name is used. If the columns slice is empty, the table's columns are used.
// The method returns an error if writing the metadata , log fails or if the table is invalid.
func (t *Table) CreateTable(name string, columns []Column, m *TableMetadata, d *DiskManager, l *TransactionLog, p *Page) error {
	t.isLocked = true
	t.RWMutex.Lock()
	defer t.RWMutex.Unlock()
	defer func() {
		t.isLocked = false
	}()
	if name == "" {
		name = t.Name
	}
	if len(columns) == 0 {
		columns = t.Columns
	}

	t.SetName(name)
	t.SetColumns(columns)
	t.SetRowsCount(0)
	t.SetSize(0)

	err := t.ValidateTable(m, d)

	if err != nil {
		return err
	}

	id := t.GenerateID()
	t.ID = id

	fmt.Printf("Creating table %s\n with columns %v having id %s", name, columns, id.String())

	r := NewRetryConf(3, 5*time.Second)

	err = t.allocateResources(d, p)
	if err != nil {
		d.rollbackResources()
		return err
	}

	err = m.writeMetaDataWithRetry(t, d, r)
	if err != nil {
		d.rollbackResources()
		t.cleanup(d)
		return err
	}

	err = l.writeCreateTableLogWithRetry(t, d, r)
	if err != nil {
		d.rollbackResources()
		t.cleanup(d)
		return err
	}

	return nil
}

// --------------------------------------------------------------------------Errors for the table validation-------------------------------------------

var (
	errTableNameEmpty     = errors.New("table name cannot be empty")
	errColumnsEmpty       = errors.New("columns cannot be empty")
	errDuplicateColumn    = errors.New("duplicate column name")
	errTableNameTooShort  = errors.New("table name must be at least 3 characters long")
	errTableNameTooLong   = errors.New("table name must be at most 64 characters long")
	errInvalidDataType    = errors.New("invalid data type")
	errTableAlreadyExists = errors.New("table already exists")
	ErrResourceAllocation = errors.New("resource allocation failed")
	errInvalidColumnName  = errors.New("column name cannot be empty")
	errColumnNameTooShort = errors.New("column name must be at least 3 characters long")
	errColumnNameTooLong  = errors.New("column name must be at most 64 characters long")
)

// ValidateTable checks if the table name is valid and the columns are valid.
// It returns an error if the table name is empty, too short, or too long,
// or if the columns are empty, or if there are duplicate column names,
// or if a column data type is empty, or if the data type is invalid.
func (t *Table) ValidateTable(m *TableMetadata, d *DiskManager) error {
	exists := m.isTableExists(t.Name, d)
	fmt.Printf("Table exists: %v\n", exists)
	if exists {
		return errTableAlreadyExists
	}

	if t.Name == "" {
		return errTableNameEmpty
	}

	if len(t.Name) < 2 {
		return errTableNameTooShort
	}

	if len(t.Name) > 64 {
		return errTableNameTooLong
	}

	if len(t.Columns) == 0 {
		return errColumnsEmpty
	}

	columnNames := make(map[string]bool)
	for _, column := range t.Columns {
		if column.Name == "" {
			return errInvalidColumnName
		}

		if len(column.Name) < 2 {
			return errColumnNameTooShort
		}

		if len(column.Name) > 64 {
			return errColumnNameTooLong
		}

		if _, exists := columnNames[column.Name]; exists {
			return errDuplicateColumn
		}
		columnNames[column.Name] = true

		if column.DataType != Int && column.DataType != String && column.DataType != Bool {
			return errInvalidDataType
		}
	}

	return nil
}

func (p *Page) GeneratePageID() uint64 {
	return 0
}

// ------------------------------------------------------------------Resource Allocation and Rollback ------------------------------------------------------------------

// allocateResources allocates resources for the table. It creates a page directory
// for the table and writes an initial page to the disk. The initial page is
// allocated with the given page size and zeroed. If there is an error writing the
// page, it returns an error wrapping ErrResourceAllocation.
func (t *Table) allocateResources(d *DiskManager, p *Page) error {
	pageDir := &PageDirectory{
		tableID: t.ID,
		pages:   make(map[uint64]*Page),
	}

	page := &Page{
		ID:       p.GeneratePageID(),
		Size:     d.PageSize,
		IsDirty:  false,
		Data:     make([]byte, d.PageSize),
		PinCount: 0,
	}

	if err := d.WritePage(*page); err != nil {
		fmt.Printf("Error writing page: %v\n", err)
		return ErrResourceAllocation
	}

	pageDir.pages[page.ID] = page
	t.pageDirectory = pageDir

	return nil
}

// rollbackResources should remove all the resources allocated for the table
func (d *DiskManager) rollbackResources() {
	// TODO: implement rollbackResources
}

// cleanup removes all the resources allocated for the table. It deletes all the pages in the page directory.
func (t *Table) cleanup(d *DiskManager) {
	if t.pageDirectory != nil {
		for _, page := range t.pageDirectory.pages {
			d.DeletePage(*page)
		}
	}
}

// DeletePage deletes a page from the disk.
func (d *DiskManager) DeletePage(page Page) error {
	return nil
}

var (
	errInvalidPageSize = errors.New("invalid page Size")
	errSeekingFile     = errors.New("seeking failed, invalid argument")
	errWritingFile     = errors.New("error writing to the file")
	errPartialWrite    = errors.New("partial data was written and error occurred")
)

// WritePage writes the given page's data to the data file associated with the DiskManager.
// It ensures that the page data size matches the expected page size, seeks to the correct
// offset based on the page ID, and writes the data to the file. It returns an error if the
// page size is invalid, seeking fails, writing fails, or if the number of written bytes
// does not match the page size. The function also syncs the data file to ensure data integrity.
func (d *DiskManager) WritePage(page Page) error {
	if len(page.Data) != int(d.PageSize) {
		fmt.Printf("invalid page size: expected %d, got %d", d.PageSize, len(page.Data))
		return errInvalidPageSize
	}

	offset := page.ID * d.PageSize
	if _, err := d.DataFile.Seek(int64(offset), io.SeekStart); err != nil {
		fmt.Printf("error seeking file %v\n", err)
		return errSeekingFile
	}

	written, err := d.DataFile.Write(page.Data)
	if err != nil {
		fmt.Printf("write error: %v", err)
		return errWritingFile
	}

	if written != int(d.PageSize) {
		fmt.Printf("partial write: wrote %d of %d bytes", written, d.PageSize)
		return errPartialWrite
	}

	return d.DataFile.Sync()
}

// ------------------------------------------------------------------Metadata Operations for Create Table ------------------------------------------------------------------

// writeMetaDataWithRetry writes the table's metadata to the metadata file
// associated with the provided DiskManager with a retry mechanism. It first
// attempts to write the metadata using WriteMetaData. If it fails, it retries
// the operation up to the number of times specified in the RetryConf using
// RetryWriteMetaData. It returns an error if all retries fail.
func (m *TableMetadata) writeMetaDataWithRetry(t *Table, d *DiskManager, r *RetryConf) error {
	err := m.WriteCreateTableMetaData(t, d)
	if err != nil {
		return m.RetryWriteCreateTableMetaData(t, d, r)
	}
	return nil
}

// WriteCreateTableMetaData writes the table's metadata to the metadata file associated with the
// provided DiskManager. It writes the table's ID, name, columns, size, and row count
// to the file in the format specified in TableFormat. It returns an error if writing
// to the file fails.
func (m *TableMetadata) WriteCreateTableMetaData(t *Table, d *DiskManager) error {
	_, err := d.MetaDataFile.Seek(0, io.SeekEnd)
	if err != nil {
		return errSeekingFile
	}

	columnFormats := make([]*ColumnFormat, len(t.Columns))
	for i, col := range t.Columns {
		columnFormats[i] = &ColumnFormat{
			Name:     col.Name,
			DataType: DataTypesFormat(col.DataType),
		}
	}

	metadata := &TableFormat{
		Id:        t.ID.String(),
		Name:      t.Name,
		Columns:   columnFormats,
		Size:      t.Size,
		RowsCount: t.RowsCount,
	}

	data, err := proto.Marshal(metadata)
	if err != nil {
		return err
	}

	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(len(data)))
	if _, err := d.MetaDataFile.Write(buf); err != nil {
		return err
	}

	writtenBytesCount, err := d.MetaDataFile.Write(data)
	if err != nil {
		return errWritingMetaData
	}
	fmt.Printf("Wrote %d bytes to metadata file\n", writtenBytesCount)
	return nil
}

// RetryWriteCreateTableMetaData retries writing the table's metadata, including its name, columns, size,
// and row count, to the metadata file associated with the provided DiskManager up to the
// number of times specified in the RetryConf. If all retries fail, it returns an error.
func (m *TableMetadata) RetryWriteCreateTableMetaData(t *Table, d *DiskManager, r *RetryConf) error {
	start := time.Now()
	count := 0
	for time.Since(start) < r.Interval*time.Duration(r.MaxRetries) {
		err := m.WriteCreateTableMetaData(t, d)
		if err == nil {
			return nil
		}
		count++
		time.Sleep(r.Interval)
	}
	return errWritingMetaData
}

// TODO - This should be optimal for read heavy calls, or when there are too many tables in the disk
// ReadMetaData reads the table metadata from the metadata file associated with
// the provided DiskManager. It iterates over the file, reading each table's
// metadata and unmarshalling it into a TableFormat object. Each valid table's
// metadata is converted into a map with keys "Name", "Size", "RowsCount", and
// "Columns", and added to a slice of maps. If no valid table metadata is found,
// it returns an error. If reading from the file fails, it returns an error.
func (m *TableMetadata) ReadTableMetaData(d *DiskManager) ([]map[string]interface{}, error) {
	var offset int64
	tables := make([]map[string]interface{}, 0)

	for {
		buf := make([]byte, 4)
		if _, err := d.MetaDataFile.ReadAt(buf, offset); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		itemSize := binary.LittleEndian.Uint32(buf)
		offset += 4

		item := make([]byte, itemSize)
		if _, err := d.MetaDataFile.ReadAt(item, offset); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		tableFormat := &TableFormat{}
		err := proto.Unmarshal(item, tableFormat)
		if err != nil {
			offset += int64(itemSize)
			continue
		}

		result := make(map[string]interface{})
		result["Name"] = tableFormat.Name
		result["Size"] = int(tableFormat.Size)
		result["RowsCount"] = int(tableFormat.RowsCount)

		columns := make([]string, len(tableFormat.Columns))
		for i, col := range tableFormat.Columns {
			columns[i] = col.Name
		}
		result["Columns"] = columns

		tables = append(tables, result)
		offset += int64(itemSize)
	}

	if len(tables) == 0 {
		return nil, fmt.Errorf("no valid table metadata found")
	}

	return tables, nil
}

// ------------------------------------------------------------------Log Operations for Create Table ------------------------------------------------------------------

// writeCreateTableLogWithRetry writes the create table log to the log file
// associated with the provided DiskManager with retry mechanism. If the
// write fails, it retries up to the number of times specified in the RetryConf.
// If all retries fail, it returns an error.
func (l *TransactionLog) writeCreateTableLogWithRetry(t *Table, d *DiskManager, r *RetryConf) error {
	err := l.WriteCreateTableLog(t, d)
	if err != nil {
		return l.RetryWriteCreateTableLog(t, d, r)
	}
	return nil
}

// WriteCreateTableLog writes the create table log to the log file associated with the
// provided DiskManager. It appends the log to the file, and returns an error if
// writing to the file fails. The log contains the table's ID, name, columns, size,
// and row count, as well as a timestamp and the create action type.
func (l *TransactionLog) WriteCreateTableLog(t *Table, d *DiskManager) error {
	_, err := d.LogFile.Seek(0, io.SeekEnd)
	if err != nil {
		return errWritingLog
	}

	log := &TransactionLog{
		Id:         l.generateTransactionLogId(),
		Data:       []byte(fmt.Sprintf("ID: %s\nTable: %s\nColumns: %v\nSize: %d\nRowsCount: %d\n", t.ID, t.Name, t.Columns, t.Size, t.RowsCount)),
		Timestamp:  timestamppb.New(time.Now()),
		ActionType: CREATE,
	}

	data, err := proto.Marshal(log)

	if err != nil {
		fmt.Printf("Error encoding log : %v\n", err)
		return errEncodingLog
	}

	writtenBytesCount, err := d.LogFile.Write(data)

	if err != nil {
		fmt.Printf("Error writing log : %v\n", err)
		return errWritingLog
	}

	fmt.Printf("Wrote %d bytes to log file\n", writtenBytesCount)
	return nil
}

// RetryWriteCreateTableLog retries writing the create table log to the log file
// associated with the provided DiskManager up to the number of times specified
// in the RetryConf. If all retries fail, it returns an error.
func (l *TransactionLog) RetryWriteCreateTableLog(t *Table, d *DiskManager, r *RetryConf) error {
	start := time.Now()
	count := 0
	for time.Since(start) < r.Interval*time.Duration(r.MaxRetries) {
		err := l.WriteCreateTableLog(t, d)
		if err == nil {
			return nil
		}
		count++
		time.Sleep(r.Interval)
	}
	return errWritingLog
}

// generateTransactionLogId generates a unique transaction log ID by seeding
// the random number generator with the current time and returning a randomly
// generated uint64 value. This ensures that each transaction log ID is
// distinct.
func (l *TransactionLog) generateTransactionLogId() uint64 {
	rand.Seed(time.Now().UnixNano())
	randomUint64 := rand.Uint64()
	return randomUint64
}

// ----------------------------------------------------------------------------Utility Functions for table and its metadata --------------------------------------------------------

// isTableExists checks if a table with the given name exists in the metadata.
// It reads the metadata using the provided DiskManager and returns true if
// a table with the specified name is found, or false otherwise. If an error
// occurs while reading metadata, it returns false.
func (m *TableMetadata) isTableExists(name string, d *DiskManager) bool {
	metadata, err := m.ReadTableMetaData(d)
	if err != nil {
		return false
	}

	for _, table := range metadata {
		if table["Name"] == name {
			return true
		}
	}
	return false
}

// isColumnExists checks if a column with the given name exists in any table in the metadata.
// It reads the metadata using the provided DiskManager and returns true if
// a column with the specified name is found, or false otherwise. If an error
// occurs while reading metadata, it returns false.
func (m *TableMetadata) isColumnExists(name string, d *DiskManager) bool {
	metadata, err := m.ReadTableMetaData(d)
	if err != nil {
		return false
	}

	for _, table := range metadata {
		columns := table["Columns"].([]string)
		for _, column := range columns {
			if column == name {
				return true
			}
		}
	}
	return false
}
