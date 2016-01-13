package models

import (
	"reflect"
	"testing"

	"github.com/NYTimes/sqliface"
)

func TestScanData(t *testing.T) {

	tests := []struct {
		name string

		given *sqliface.MockRows

		want       []Data
		wantErr    error
		wantClosed bool
	}{
		{
			"ScanData OK",

			sqliface.NewMockRows(
				sqliface.MockRow{
					uint64(123),
					"JP",
				},
				sqliface.MockRow{
					uint64(1234),
					"George",
				}),

			[]Data{
				Data{123, "JP"},
				Data{1234, "George"},
			},
			nil,
			false,
		},
		{
			"ScanData scan error",

			sqliface.NewMockRows(
				sqliface.MockRow{
					uint32(1234),
					"",
				}),

			[]Data(nil),
			sqliface.NewTypeError("uint64", uint32(1234)),
			true,
		},
	}

	for _, test := range tests {
		got, gotErr := scanDatas(test.given)

		if test.wantClosed && !test.given.Closed {
			t.Errorf("TEST[%s] scanDatas(..) did not close the rows struct after an error", test.name)
		}

		if !reflect.DeepEqual(gotErr, test.wantErr) {
			t.Errorf("TEST[%s] scanDatas(..) got error of:\n%#v\n\nexpected:\n%#v:", test.name, gotErr, test.wantErr)
			continue
		}

		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("TEST[%s] scanDatas(..) got:\n%#v\n expected:\n%#v", test.name, got, test.want)
		}
	}
}
