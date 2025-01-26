package internal

import (
	"os"
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
	Name            string
	DataType        DataTypes
	IsNullable      bool
	IsPrimaryKey    bool
	IsUnique        bool
	IsAutoIncrement bool
	DefaultValue    string
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

type AlterTable struct {
	Name                   string
	DeletedColumns         []Column
	AddedColumns           []Column
	RenamedColumns         []Column
	UpdatedColumnDataTypes []Column
}
