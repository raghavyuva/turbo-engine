package internal

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

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

var (
	errWritingMetaData = errors.New("error writing metadata")
	errWritingLog      = errors.New("error writing log")
)

// CreateTable creates a table with the given name and columns, and writes the
// corresponding metadata and log entries. If the name is empty, the table's
// name is used. If the columns slice is empty, the table's columns are used.
// The method returns an error if writing the metadata , log fails or if the table is invalid.
func (t *Table) CreateTable(name string, columns []Column, m *MetaData, d *DiskManager, l *TransactionLog, p *Page) error {
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

// error messages for table validation
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
func (t *Table) ValidateTable(m *MetaData, d *DiskManager) error {
	exists := m.isTableExists(t.Name, d)

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

// writeMetaDataWithRetry writes the table's metadata to the metadata file
// associated with the provided DiskManager with a retry mechanism. It first
// attempts to write the metadata using WriteMetaData. If it fails, it retries
// the operation up to the number of times specified in the RetryConf using
// RetryWriteMetaData. It returns an error if all retries fail.
func (m *MetaData) writeMetaDataWithRetry(t *Table, d *DiskManager, r *RetryConf) error {
	err := m.WriteMetaData(t, d)
	if err != nil {
		return m.RetryWriteMetaData(t, d, r)
	}
	return nil
}

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

// WriteMetaData writes the table's metadata to the metadata file
// associated with the provided DiskManager. It appends the metadata to
// the file, and returns an error if writing to the file fails.
func (m *MetaData) WriteMetaData(t *Table, d *DiskManager) error {
	_, err := d.MetaDataFile.Seek(0, io.SeekEnd)
	if err != nil {
		return errSeekingFile
	}

	writtenBytesCount, err := d.MetaDataFile.WriteString(fmt.Sprintf(
		"Table: %s\nColumns: %v\nSize: %d\nRowsCount: %d\nID: %s\n",
		t.Name, t.Columns, t.Size, t.RowsCount, t.ID))
	if err != nil {
		return errWritingMetaData
	}

	fmt.Printf("Wrote %d bytes to metadata file\n", writtenBytesCount)
	return nil
}

// ReadMetaData reads the metadata from the file associated with the provided DiskManager,
// and returns the metadata as a map of string keys to interface{} values.
// The map contains the following keys: "Name", "Columns", "Size", and "RowsCount".
// The values are the corresponding values from the metadata file, converted to
// Go values according to the following rules:
// - "Size" and "RowsCount" are converted to int.
// - "Columns" is split into a slice of strings.
// - Other keys are left as strings.
// If there is an error reading the metadata file, it returns an error.
func (m *MetaData) ReadMetaData(d *DiskManager) (map[string]interface{}, error) {
	metadata := make([]byte, 1024)
	n, err := d.MetaDataFile.Read(metadata)
	if err != nil {
		return nil, err
	}

	result := make(map[string]interface{})
	lines := strings.Split(string(metadata[:n]), "\n")

	for _, line := range lines {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ": ", 2)
		if len(parts) != 2 {
			continue
		}

		key := parts[0]
		value := parts[1]

		switch key {
		case "Size", "RowsCount":
			if v, err := strconv.Atoi(value); err == nil {
				result[key] = v
			}
		case "Columns":
			result[key] = strings.Split(strings.Trim(value, "[]"), " ")
		default:
			result[key] = value
		}
	}

	return result, nil
}

// isTableExists checks if a table with the given name exists in the metadata file
// associated with the provided DiskManager. It returns true if the table exists,
// and false otherwise. If there is an error reading the metadata file, it returns
// false.
func (m *MetaData) isTableExists(name string, d *DiskManager) bool {
	metadata, err := m.ReadMetaData(d)
	if err != nil {
		return false
	}

	if metadata["Table"] == name {
		return true
	}
	return false
}

// RetryWriteMetaData retries writing the table's metadata, including its name, columns, size,
// and row count, to the metadata file associated with the provided DiskManager up to the
// number of times specified in the RetryConf. If all retries fail, it returns an error.
func (m *MetaData) RetryWriteMetaData(t *Table, d *DiskManager, r *RetryConf) error {
	start := time.Now()
	count := 0
	for time.Since(start) < r.Interval*time.Duration(r.MaxRetries) {
		err := m.WriteMetaData(t, d)
		if err == nil {
			return nil
		}
		count++
		time.Sleep(r.Interval)
	}
	return errWritingMetaData
}

// WriteCreateTableLog writes the create table log to the log file
// It appends the table name, columns, size and rows count to the log file
// and returns an error if there is an error while writing to the log file
func (l *TransactionLog) WriteCreateTableLog(t *Table, d *DiskManager) error {
	_, err := d.LogFile.Seek(0, io.SeekEnd)
	if err != nil {
		return errWritingLog
	}
	writtenBytesCount, err := d.LogFile.Write([]byte(fmt.Sprintf("ID: %s\nTable: %s\nColumns: %v\nSize: %d\nRowsCount: %d\nType: %s\n", t.ID, t.Name, t.Columns, t.Size, t.RowsCount, "CREATE")))
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
