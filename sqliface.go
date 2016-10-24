package sqliface

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Row is an interface implemented by sql.Row, sql.Rows, and MockRow.
type Row interface {
	Scan(dest ...interface{}) error
}

// Rows is an interface implemented by sql.Rows and MockRows.
type Rows interface {
	Next() bool
	Scan(dest ...interface{}) error
	Err() error
	Close() error
}

// MockRow is database row that implements Row to help test applications
// using database/sql.
type MockRow []interface{}

// MockRows is a set of database rows that implements Rows to help test applications
// using database/sql.
type MockRows struct {
	Rows   []MockRow
	Index  int
	Error  error
	Closed bool
}

// NewMockRows will create a new MockRows instance with the given MockRow set.
func NewMockRows(rows ...MockRow) *MockRows {
	return &MockRows{rows, -1, nil, false}
}

// Next will shift the Index of MockRows to the next MockRow in the set.
func (ms *MockRows) Next() bool {
	ms.Index++
	return (len(ms.Rows) != 0) && (ms.Index < len(ms.Rows))
}

// Err will return the given Error in the MockRows object.
func (ms *MockRows) Err() error {
	return ms.Error
}

// Close will set the Close flag in MockRows so users can verify the method
// gets called.
func (ms *MockRows) Close() error {
	ms.Closed = true
	return nil
}

// Scan will attempt to scan the current MockRow into the given interfaces.
func (ms *MockRows) Scan(dest ...interface{}) error {
	if ms.Index >= len(ms.Rows) {
		return errors.New("nothing left to scan in mock row")
	}
	return ms.Rows[ms.Index].Scan(dest...)
}

// Scan is an implementation for a fake database row.
func (mr MockRow) Scan(dest ...interface{}) error {
	lenMr := len(mr)
	lenDest := len(dest)

	if lenMr != lenDest {
		return fmt.Errorf("Mock row len %v does not match dest len %v", lenMr, lenDest)
	}

	for i := 0; i < lenMr; i++ {

		// Find the pointer type of the destination value, it should match the
		// source value from the mock row. If there is a bad match or a type we
		// haven't implemented, we'll return an error.
		switch dVal := dest[i].(type) {

		// If the type you want isn't here, just add a stanza for it

		case *int:
			mrVal, ok := mr[i].(int)
			if !ok {
				return NewTypeError("int", mr[i])
			}
			*dVal = mrVal

		case *int8:
			mrVal, ok := mr[i].(int8)
			if !ok {
				return NewTypeError("int8", mr[i])
			}
			*dVal = mrVal

		case *int16:
			mrVal, ok := mr[i].(int16)
			if !ok {
				return NewTypeError("int16", mr[i])
			}
			*dVal = mrVal

		case *int32:
			mrVal, ok := mr[i].(int32)
			if !ok {
				return NewTypeError("int32", mr[i])
			}
			*dVal = mrVal

		case *int64:
			mrVal, ok := mr[i].(int64)
			if !ok {
				return NewTypeError("int64", mr[i])
			}
			*dVal = mrVal

		case *uint:
			mrVal, ok := mr[i].(uint)
			if !ok {
				return NewTypeError("uint", mr[i])
			}
			*dVal = mrVal

		case *uint8:
			mrVal, ok := mr[i].(uint8)
			if !ok {
				return NewTypeError("uint8", mr[i])
			}
			*dVal = mrVal

		case *uint16:
			mrVal, ok := mr[i].(uint16)
			if !ok {
				return NewTypeError("uint16", mr[i])
			}
			*dVal = mrVal

		case *uint32:
			mrVal, ok := mr[i].(uint32)
			if !ok {
				return NewTypeError("uint32", mr[i])
			}
			*dVal = mrVal

		case *uint64:
			mrVal, ok := mr[i].(uint64)
			if !ok {
				return NewTypeError("uint64", mr[i])
			}
			*dVal = mrVal

		case *float32:
			mrVal, ok := mr[i].(float32)
			if !ok {
				return NewTypeError("float32", mr[i])
			}
			*dVal = mrVal

		case *float64:
			mrVal, ok := mr[i].(float64)
			if !ok {
				return NewTypeError("float64", mr[i])
			}
			*dVal = mrVal

		case *string:
			mrVal, ok := mr[i].(string)
			if !ok {
				return NewTypeError("string", mr[i])
			}
			*dVal = mrVal

		case *time.Time:
			mrVal, ok := mr[i].(time.Time)
			if !ok {
				return NewTypeError("time.Time", mr[i])
			}
			*dVal = mrVal

		case *mysql.NullTime:
			dVal.Time, dVal.Valid = mr[i].(time.Time)

		case *bool:
			mrVal, ok := mr[i].(bool)
			if !ok {
				return NewTypeError("bool", mr[i])
			}
			*dVal = mrVal

		case *sql.RawBytes:
			mrVal, ok := mr[i].([]byte)
			if !ok {
				return NewTypeError("sql.RawBytes", mr[i])
			}
			*dVal = mrVal

		case *sql.NullString:
			mrVal, ok := mr[i].(string)
			if !ok {
				dVal.Valid = false
				dVal.String = ""
			} else {
				dVal.Valid = true
				dVal.String = mrVal
			}

		case *sql.NullInt64:
			mrVal, ok := mr[i].(int64)
			if !ok {
				dVal.Valid = false
				dVal.Int64 = int64(0)
			} else {
				dVal.Valid = true
				dVal.Int64 = mrVal
			}

		default:
			return fmt.Errorf("scanning type not yet supported for %#v at index %d - , but you can add the implementation in MockRow.Scan()",
				dVal, i)
		}
	}

	return nil
}

// TypeError is returned by MockRow.Scan when the type it's attempting
// to scan into does not match the type in the MockRow.
type TypeError struct {
	Expected string
	Got      interface{}
}

// NewTypeError returns a TypeError instance. This can helpful to use
// when testing for errors while scanning from databases.
func NewTypeError(exp string, got interface{}) *TypeError {
	return &TypeError{exp, got}
}

// Error allows TypeError to implement the error interface.
func (t *TypeError) Error() string {
	return fmt.Sprintf("expected %s, but got %v of type %T", t.Expected, t.Got, t.Got)
}

// Execer is an interface that both sql.Tx and sql.DB implement. Using this
// interface will allow you to pass either into a function.
type Execer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

// ExecCloser implements the Execer interface with an additional Close method. Using this will allow you
// to test simple uses of sql.DB in a service.
type ExecCloser interface {
	Execer
	io.Closer
}
