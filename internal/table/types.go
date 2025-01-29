package internal

import (
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
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

const (
	CREATE ActionType = iota
	INSERT
	UPDATE
	DELETE
)

func NewTransactionLog() *TransactionLog {
	return &TransactionLog{
		Id:         0,
		Data:       nil,
		Timestamp:  timestamppb.New(time.Now()),
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
	Name            string
	DataType        DataTypes
	IsNullable      bool
	IsPrimaryKey    bool
	IsUnique        bool
	IsAutoIncrement bool
	DefaultValue    string
}

func NewColumn(name string, dataType DataTypes, isNullable bool, isPrimaryKey bool, isUnique bool, isAutoIncrement bool, defaultValue string) *Column {
	return &Column{
		Name:            name,
		DataType:        dataType,
		IsNullable:      isNullable,
		IsPrimaryKey:    isPrimaryKey,
		IsUnique:        isUnique,
		IsAutoIncrement: isAutoIncrement,
		DefaultValue:    defaultValue,
	}
}

type DiskManager struct {
	PageSize     uint64
	DataFile     *os.File
	MetaDataFile *os.File
	LogFile      *os.File
}

var (
	dataFileName     = "./database/data.db"
	metadataFileName = "./database/metadata.db"
	logFileName      = "./database/transaction_log.db"
)

func NewDiskManager() *DiskManager {
	dataFile, _ := os.OpenFile(dataFileName, os.O_CREATE|os.O_RDWR, 0666)
	metadataFile, _ := os.OpenFile(metadataFileName, os.O_CREATE|os.O_RDWR, 0666)
	logFile, _ := os.OpenFile(logFileName, os.O_CREATE|os.O_RDWR, 0666)
	return &DiskManager{
		PageSize:     4096,
		DataFile:     dataFile,
		MetaDataFile: metadataFile,
		LogFile:      logFile,
	}
}

type TableMetadata struct {
	Tables []*Table
}

func NewMetaData() *TableMetadata {
	return &TableMetadata{
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

func NewTable(name string, columns []Column, pageDirectory *PageDirectory, Size, RowsCount uint64) *Table {
	return &Table{
		ID:            uuid.New(),
		Name:          name,
		Columns:       columns,
		Size:          Size,
		RowsCount:     RowsCount,
		pageDirectory: pageDirectory,
	}
}

type RetryConf struct {
	MaxRetries int
	Interval   time.Duration
}

func NewRetryConf(maxRetries int, interval time.Duration) *RetryConf {
	return &RetryConf{MaxRetries: maxRetries, Interval: interval}
}

type AlterTable struct {
	Name                   string
	DeletedColumns         []string
	AddedColumns           []Column
	RenamedColumns         []RenamedColumns
	UpdatedColumnDataTypes []Column
}

type RenamedColumns struct {
	OldName string
	NewName string
}
