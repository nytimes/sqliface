package sqliface

import (
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

type (
	testStruct struct {
		Int        int
		Int8       int8
		Int16      int16
		Int32      int32
		Int64      int64
		Uint       uint
		Uint8      uint8
		Uint16     uint16
		Uint32     uint32
		Uint64     uint64
		String     string
		Time       time.Time
		NullTime   mysql.NullTime
		Bool       bool
		NullString sql.NullString
		NullInt64  sql.NullInt64
		Float32    float32
		Float64    float64
		Bytes      sql.RawBytes

		Custom *customStruct
	}

	customStruct struct{}
)

var testTime = time.Date(2016, 1, 11, 0, 0, 0, 0, time.UTC)

func TestMisc(t *testing.T) {
	row := MockRow{123}
	rows := NewMockRows(row)

	rows.Next()
	// verify next can only be called once
	if rows.Next() {
		t.Error("MockRows should not have been able to advance the Index")
	}

	var one, two int
	if rows.Scan(&one, &two) == nil {
		t.Error("MockRows should have errored after bad scan")
	}
	if row.Scan(&one, &two) == nil {
		t.Error("MockRow should have errored after bad scan")
	}

	rows.Close()
	// closed flag should be set after calling Close()
	if !rows.Closed {
		t.Error("MockRows should not have been marked as close")
	}

	rows.Error = errors.New("nopes!")
	if rows.Err() == nil {
		t.Error("MockRows should have returned the expected error")
	}

}

func TestScanCustom(t *testing.T) {
	rows := NewMockRows(MockRow{&customStruct{}})

	if !rows.Next() {
		t.Fatal("unable to advance MockRows")
	}

	var got customStruct
	err := rows.Scan(&got)
	if err == nil {
		t.Error("expected error for incompatible type, got none")
	}
}

func TestScanBadTypes(t *testing.T) {
	tests := []struct {
		givenRow MockRow

		wantErr error
	}{
		{
			MockRow{
				123,
				int8(123),
				int16(456),
				int32(321),
				int64(456),
				uint(123),
				uint8(45),
				uint16(321),
				uint32(321),
				uint64(654),
				"string",
				testTime,
				testTime,
				true,
				"nullstring",
				int64(123),
				float32(1.1),
				float64(2.3),
				[]byte("yo"),
			},

			nil,
		},
		{
			MockRow{
				int64(123),
				int8(123),
				int16(456),
				int32(321),
				int64(456),
				uint(123),
				uint8(45),
				uint16(321),
				uint32(321),
				uint64(654),
				"string",
				testTime,
				testTime,
				true,
				"nullstring",
				int64(123),
				float32(1.1),
				float64(2.3),
				[]byte("yo"),
			},

			&TypeError{"int", int64(123)},
		},
		{
			MockRow{
				123,
				456,
				int16(456),
				int32(321),
				int64(456),
				uint(123),
				uint8(45),
				uint16(321),
				uint32(321),
				uint64(654),
				"string",
				testTime,
				testTime,
				true,
				"nullstring",
				int64(123),
				float32(1.1),
				float64(2.3),
				[]byte("yo"),
			},

			&TypeError{"int8", 456},
		},
		{
			MockRow{
				123,
				int8(123),
				321,
				int32(321),
				int64(456),
				uint(123),
				uint8(45),
				uint16(321),
				uint32(321),
				uint64(654),
				"string",
				testTime,
				testTime,
				true,
				"nullstring",
				int64(123),
				float32(1.1),
				float64(2.3),
				[]byte("yo"),
			},

			&TypeError{"int16", 321},
		},
		{
			MockRow{
				123,
				int8(123),
				int16(456),
				456,
				int64(456),
				uint(123),
				uint8(45),
				uint16(321),
				uint32(321),
				uint64(654),
				"string",
				testTime,
				testTime,
				true,
				"nullstring",
				int64(123),
				float32(1.1),
				float64(2.3),
				[]byte("yo"),
			},

			&TypeError{"int32", 456},
		},
		{
			MockRow{
				123,
				int8(123),
				int16(456),
				int32(321),
				456,
				uint(123),
				uint8(45),
				uint16(321),
				uint32(321),
				uint64(654),
				"string",
				testTime,
				testTime,
				true,
				"nullstring",
				int64(123),
				float32(1.1),
				float64(2.3),
				[]byte("yo"),
			},

			&TypeError{"int64", 456},
		},
		{
			MockRow{
				123,
				int8(123),
				int16(456),
				int32(321),
				int64(456),
				-123,
				uint8(45),
				uint16(321),
				uint32(321),
				uint64(654),
				"string",
				testTime,
				testTime,
				true,
				"nullstring",
				int64(123),
				float32(1.1),
				float64(2.3),
				[]byte("yo"),
			},

			&TypeError{"uint", -123},
		},
		{
			MockRow{
				123,
				int8(123),
				int16(456),
				int32(321),
				int64(456),
				uint(123),
				321,
				uint16(321),
				uint32(321),
				uint64(654),
				"string",
				testTime,
				testTime,
				true,
				"nullstring",
				int64(123),
				float32(1.1),
				float64(2.3),
				[]byte("yo"),
			},

			&TypeError{"uint8", 321},
		},
		{
			MockRow{
				123,
				int8(123),
				int16(456),
				int32(321),
				int64(456),
				uint(123),
				uint8(45),
				321,
				uint32(321),
				uint64(654),
				"string",
				testTime,
				testTime,
				true,
				"nullstring",
				int64(123),
				float32(1.1),
				float64(2.3),
				[]byte("yo"),
			},

			&TypeError{"uint16", 321},
		},
		{
			MockRow{
				123,
				int8(123),
				int16(456),
				int32(321),
				int64(456),
				uint(123),
				uint8(45),
				uint16(321),
				321,
				uint64(654),
				"string",
				testTime,
				testTime,
				true,
				"nullstring",
				int64(123),
				float32(1.1),
				float64(2.3),
				[]byte("yo"),
			},

			&TypeError{"uint32", 321},
		},
		{
			MockRow{
				123,
				int8(123),
				int16(456),
				int32(321),
				int64(456),
				uint(123),
				uint8(45),
				uint16(321),
				uint32(321),
				654,
				"string",
				testTime,
				testTime,
				true,
				"nullstring",
				int64(123),
				float32(1.1),
				float64(2.3),
				[]byte("yo"),
			},

			&TypeError{"uint64", 654},
		},
		{
			MockRow{
				123,
				int8(123),
				int16(456),
				int32(321),
				int64(456),
				uint(123),
				uint8(45),
				uint16(321),
				uint32(321),
				uint64(654),
				123,
				testTime,
				testTime,
				true,
				"nullstring",
				int64(123),
				float32(1.1),
				float64(2.3),
				[]byte("yo"),
			},

			&TypeError{"string", 123},
		},
		{
			MockRow{
				123,
				int8(123),
				int16(456),
				int32(321),
				int64(456),
				uint(123),
				uint8(45),
				uint16(321),
				uint32(321),
				uint64(654),
				"string",
				"string",
				testTime,
				true,
				"nullstring",
				int64(123),
				float32(1.1),
				float64(2.3),
				[]byte("yo"),
			},

			&TypeError{"time.Time", "string"},
		},
		{
			MockRow{
				123,
				int8(123),
				int16(456),
				int32(321),
				int64(456),
				uint(123),
				uint8(45),
				uint16(321),
				uint32(321),
				uint64(654),
				"string",
				testTime,
				"string",
				"WHA?",
				"nullstring",
				int64(123),
				float32(1.1),
				float64(2.3),
				[]byte("yo"),
			},

			&TypeError{"bool", "WHA?"},
		},
		{
			MockRow{
				123,
				int8(123),
				int16(456),
				int32(321),
				int64(456),
				uint(123),
				uint8(45),
				uint16(321),
				uint32(321),
				uint64(654),
				"string",
				testTime,
				testTime,
				true,
				"nullstring",
				int64(123),
				123,
				float64(2.3),
				[]byte("yo"),
			},

			&TypeError{"float32", 123},
		},
		{
			MockRow{
				123,
				int8(123),
				int16(456),
				int32(321),
				int64(456),
				uint(123),
				uint8(45),
				uint16(321),
				uint32(321),
				uint64(654),
				"string",
				testTime,
				testTime,
				true,
				"nullstring",
				int64(123),
				float32(1.1),
				"hi!",
				[]byte("yo"),
			},

			&TypeError{"float64", "hi!"},
		},
		{
			MockRow{
				123,
				int8(123),
				int16(456),
				int32(321),
				int64(456),
				uint(123),
				uint8(45),
				uint16(321),
				uint32(321),
				uint64(654),
				"string",
				testTime,
				testTime,
				true,
				"nullstring",
				int64(123),
				float32(1.1),
				float64(2.3),
				1.01,
			},

			&TypeError{"sql.RawBytes", 1.01},
		},
		{
			MockRow{
				123,
				int8(123),
				int16(456),
				int32(321),
				int64(456),
				uint(123),
				uint8(45),
				uint16(321),
				uint32(321),
				uint64(654),
				"string",
				testTime,
				testTime,
				true,
				"nullstring",
				int64(123),
				float32(1.1),
				float64(2.3),
				[]byte("yo"),
			},

			nil,
		},
		{
			MockRow{
				123,
				int8(123),
				int16(456),
				int32(321),
				int64(456),
				uint(123),
				uint8(45),
				uint16(321),
				uint32(321),
				uint64(654),
				"string",
				testTime,
				testTime,
				true,
				0.0,
				int64(123),
				float32(1.1),
				float64(2.3),
				[]byte("yo"),
			},

			nil,
		},
		{
			MockRow{
				123,
				int8(123),
				int16(456),
				int32(321),
				int64(456),
				uint(123),
				uint8(45),
				uint16(321),
				uint32(321),
				uint64(654),
				"string",
				testTime,
				testTime,
				true,
				"string",
				0.0,
				float32(1.1),
				float64(2.3),
				[]byte("yo"),
			},

			nil,
		},
	}

	for testnum, test := range tests {
		rows := NewMockRows(test.givenRow)

		if !rows.Next() {
			t.Fatal("unable to advance MockRows")
		}

		var got testStruct
		err := rows.Scan(
			&got.Int,
			&got.Int8,
			&got.Int16,
			&got.Int32,
			&got.Int64,
			&got.Uint,
			&got.Uint8,
			&got.Uint16,
			&got.Uint32,
			&got.Uint64,
			&got.String,
			&got.Time,
			&got.NullTime,
			&got.Bool,
			&got.NullString,
			&got.NullInt64,
			&got.Float32,
			&got.Float64,
			&got.Bytes,
		)

		if !reflect.DeepEqual(test.wantErr, err) {
			t.Errorf("TEST[%d] expected error to be \n'%#v', got \n'%#v'", testnum, test.wantErr, err)
		}
	}
}

func TestScanNoNull(t *testing.T) {
	rows := NewMockRows(MockRow{
		123,
		int8(123),
		int16(456),
		int32(321),
		int64(456),
		uint(123),
		uint8(45),
		uint16(321),
		uint32(321),
		uint64(654),
		"string",
		testTime,
		testTime,
		true,
		"nullstring",
		int64(123),
		float32(1.1),
		float64(2.3),
		[]byte("yo"),
	})

	want := testStruct{
		123,
		int8(123),
		int16(456),
		int32(321),
		int64(456),
		uint(123),
		uint8(45),
		uint16(321),
		uint32(321),
		uint64(654),
		"string",
		testTime,
		mysql.NullTime{testTime, true},
		true,
		sql.NullString{"nullstring", true},
		sql.NullInt64{123, true},
		float32(1.1),
		float64(2.3),
		[]byte("yo"),
		nil,
	}

	if !rows.Next() {
		t.Fatal("unable to advance MockRows")
	}

	var got testStruct
	err := rows.Scan(
		&got.Int,
		&got.Int8,
		&got.Int16,
		&got.Int32,
		&got.Int64,
		&got.Uint,
		&got.Uint8,
		&got.Uint16,
		&got.Uint32,
		&got.Uint64,
		&got.String,
		&got.Time,
		&got.NullTime,
		&got.Bool,
		&got.NullString,
		&got.NullInt64,
		&got.Float32,
		&got.Float64,
		&got.Bytes)
	if err != nil {
		t.Error("unexpected error:", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("expected:\n%#v\ngot:\n%#v", want, got)
	}
}

func TestTypeError(t *testing.T) {
	err := NewTypeError("int", "hi mom")
	want := "expected int, but got hi mom of type string"
	got := err.Error()

	if want != got {
		t.Errorf("expected '%#v', got '%#v'", want, got)
	}
}
