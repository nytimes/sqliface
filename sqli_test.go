package sqli

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
		Int64      int64
		Uint64     uint64
		Uint32     uint32
		String     string
		Time       time.Time
		NullTime   mysql.NullTime
		Bool       bool
		NullString sql.NullString
		NullInt64  sql.NullInt64
		Custom     *customStruct
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

		wantErr bool
	}{
		{
			MockRow{
				123,
				int64(456),
				uint64(654),
				uint32(321),
				"string",
				testTime,
				testTime,
				true,
				"nullstring",
				int64(123),
			},

			false,
		},
		{
			MockRow{
				int64(123),
				int64(456),
				uint64(654),
				uint32(321),
				"string",
				testTime,
				testTime,
				true,
				"nullstring",
				int64(123),
			},

			true,
		},

		{
			MockRow{
				123,
				456,
				uint64(654),
				uint32(321),
				"string",
				testTime,
				testTime,
				true,
				"nullstring",
				int64(123),
			},

			true,
		},
		{
			MockRow{
				123,
				int64(456),
				654,
				uint32(321),
				"string",
				testTime,
				testTime,
				true,
				"nullstring",
				int64(123),
			},

			true,
		},
		{
			MockRow{
				123,
				int64(456),
				uint64(654),
				321,
				"string",
				testTime,
				testTime,
				true,
				"nullstring",
				int64(123),
			},

			true,
		},
		{
			MockRow{
				123,
				int64(456),
				uint64(654),
				uint32(321),
				123,
				testTime,
				testTime,
				true,
				"nullstring",
				int64(123),
			},

			true,
		},
		{
			MockRow{
				123,
				int64(456),
				uint64(654),
				uint32(321),
				"string",
				"string",
				testTime,
				true,
				"nullstring",
				int64(123),
			},

			true,
		},
		{
			MockRow{
				123,
				int64(456),
				uint64(654),
				uint32(321),
				"string",
				testTime,
				"string",
				"WHA?",
				"nullstring",
				int64(123),
			},

			true,
		},
		{
			MockRow{
				123,
				int64(456),
				uint64(654),
				uint32(321),
				"string",
				testTime,
				testTime,
				"WHA?",
				"nullstring",
				int64(123),
			},

			true,
		},
		{
			MockRow{
				123,
				int64(456),
				uint64(654),
				uint32(321),
				"string",
				testTime,
				testTime,
				true,
				0.0,
				int64(123),
			},

			false,
		},
		{
			MockRow{
				123,
				int64(456),
				uint64(654),
				uint32(321),
				"string",
				testTime,
				testTime,
				true,
				"string",
				0.0,
			},

			false,
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
			&got.Int64,
			&got.Uint64,
			&got.Uint32,
			&got.String,
			&got.Time,
			&got.NullTime,
			&got.Bool,
			&got.NullString,
			&got.NullInt64)
		if test.wantErr {
			if err == nil {
				t.Errorf("TEST[%d] expected error, got none", testnum)
			}
		} else if err != nil {
			t.Errorf("TEST[%d] got error but expected none: %s", testnum, err)
		}
	}
}

func TestScanNoNull(t *testing.T) {
	rows := NewMockRows(MockRow{
		123,
		int64(456),
		uint64(654),
		uint32(321),
		"string",
		testTime,
		testTime,
		true,
		"nullstring",
		int64(123),
	})

	want := testStruct{
		123,
		int64(456),
		uint64(654),
		uint32(321),
		"string",
		testTime,
		mysql.NullTime{testTime, true},
		true,
		sql.NullString{"nullstring", true},
		sql.NullInt64{123, true},
		nil,
	}

	if !rows.Next() {
		t.Fatal("unable to advance MockRows")
	}

	var got testStruct
	err := rows.Scan(
		&got.Int,
		&got.Int64,
		&got.Uint64,
		&got.Uint32,
		&got.String,
		&got.Time,
		&got.NullTime,
		&got.Bool,
		&got.NullString,
		&got.NullInt64)
	if err != nil {
		t.Error("unexpected error:", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("expected:\n%#v\ngot:\n%#v", want, got)
	}
}
