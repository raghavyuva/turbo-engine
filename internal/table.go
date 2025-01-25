package internal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Page struct {
	ID       uint64
	Size     uint64
	NextPage uint64
	IsDirty  bool
	Data     []byte
	PinCount int
}

func NewPage() *Page {
	return &Page{
		ID:       0,
		Size:     4096,
		NextPage: 0,
		IsDirty:  false,
		Data:     make([]byte, 4096),
		PinCount: 0,
	}
}

type PageDirectory struct {
	tableID uuid.UUID
	pages   map[uint64]*Page
}

type ActionType int

const (
	CREATE ActionType = iota
	INSERT
	UPDATE
	DELETE
)

type TransactionLog struct {
	ID         uint64
	Data       []byte
	Timestamp  time.Time
	ActionType ActionType
}

func NewTransactionLog() *TransactionLog {
	return &TransactionLog{
		ID:         0,
		Data:       nil,
		Timestamp:  time.Now(),
		ActionType: CREATE,
	}
}

type DataTypes int

const (
	Int DataTypes = iota
	String
	Bool
)

type Column struct {
	Name     string
	DataType DataTypes
}

type DiskManager struct {
	PageSize     uint64
	DataFile     *os.File
	MetaDataFile *os.File
	LogFile      *os.File
}

func NewDiskManager() *DiskManager {
	dataFile, _ := os.OpenFile("./database/data.db", os.O_CREATE|os.O_RDWR, 0666)
	metadataFile, _ := os.OpenFile("./database/metadata.db", os.O_CREATE|os.O_RDWR, 0666)
	logFile, _ := os.OpenFile("./database/transaction_log.db", os.O_CREATE|os.O_RDWR, 0666)
	return &DiskManager{
		PageSize:     4096,
		DataFile:     dataFile,
		MetaDataFile: metadataFile,
		LogFile:      logFile,
	}
}

type MetaData struct {
	Tables []*Table
}

func NewMetaData() *MetaData {
	return &MetaData{
		Tables: []*Table{},
	}
}

type Table struct {
	ID            uuid.UUID
	Name          string
	Columns       []Column
	Size          uint64
	RowsCount     uint64
	pageDirectory *PageDirectory

	sync.RWMutex // to lock the table
	isLocked     bool
}

type RetryConf struct {
	MaxRetries int
	Interval   time.Duration
}

func NewRetryConf(maxRetries int, interval time.Duration) *RetryConf {
	return &RetryConf{MaxRetries: maxRetries, Interval: interval}
}

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
	t.SetRowsCount(0)
	t.SetSize(0)

	err := t.ValidateTable(m, d)

	if err != nil {
		return err
	}

	id := t.GenerateID()
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

	if len(t.Name) < 3 {
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
		return fmt.Errorf("%w: failed to write initial page", ErrResourceAllocation)
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

// DeletePage deletes a page from the disk. It does not return an error.
func (d *DiskManager) DeletePage(page Page) error {
	return nil
}

// WritePage writes the given page's data to the data file associated with the DiskManager.
// It ensures that the page data size matches the expected page size, seeks to the correct
// offset based on the page ID, and writes the data to the file. It returns an error if the
// page size is invalid, seeking fails, writing fails, or if the number of written bytes
// does not match the page size. The function also syncs the data file to ensure data integrity.
func (d *DiskManager) WritePage(page Page) error {
	if len(page.Data) != int(d.PageSize) {
		return fmt.Errorf("invalid page size: expected %d, got %d", d.PageSize, len(page.Data))
	}

	offset := page.ID * d.PageSize
	if _, err := d.DataFile.Seek(int64(offset), io.SeekStart); err != nil {
		return fmt.Errorf("seek error: %w", err)
	}

	written, err := d.DataFile.Write(page.Data)
	if err != nil {
		return fmt.Errorf("write error: %w", err)
	}

	if written != int(d.PageSize) {
		return fmt.Errorf("partial write: wrote %d of %d bytes", written, d.PageSize)
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
		return err
	}

	writtenBytesCount, err := d.MetaDataFile.WriteString(fmt.Sprintf(
		"Table: %s\nColumns: %v\nSize: %d\nRowsCount: %d\n",
		t.Name, t.Columns, t.Size, t.RowsCount))
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
	writtenBytesCount, err := d.LogFile.Write([]byte(fmt.Sprintf("Table: %s\nColumns: %v\nSize: %d\nRowsCount: %d\nType: %s\n", t.Name, t.Columns, t.Size, t.RowsCount, "CREATE")))
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
