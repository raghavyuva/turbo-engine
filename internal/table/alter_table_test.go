package table

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ValidateAlterTable(t *testing.T) {
	table := &AlterTable{
		Name:                   "users",
		DeletedColumns:         []string{"id"},
		AddedColumns:           []Column{{Name: "email", DataType: String}},
		RenamedColumns:         []RenamedColumns{{OldName: "name", NewName: "first_name"}},
		UpdatedColumnDataTypes: []Column{{Name: "name", DataType: String}},
	}
	m := NewMetaData()
	d := NewDiskManager()
	err := table.ValidateAlterTable(m, d)
	assert.Nil(t, err)
}
