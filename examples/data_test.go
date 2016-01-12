package models

import (
	"errors"
	"reflect"
	"testing"

	"github.com/NYTimes/sqliface"
)

func TestScanData(t *testing.T) {

	tests := []struct {
		name string

		given *sqliface.MockRows

		want       []Data
		wantErr    bool
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
			false,
			false,
		},
		{
			"ScanData scan error",

			sqliface.NewMockRows(
				sqliface.MockRow{
					uint32(1234),
					"",
				}),

			[]Data{},
			true,
			true,
		},
		{
			"ScanData rows error",

			sqliface.NewMockRows(
				sqliface.MockRow{
					uint64(1234),
					nil,
				}),

			[]Data{},
			true,
			false,
		},
	}

	for _, test := range tests {
		if test.wantErr {
			test.given.Error = errors.New("an error!")
		}

		got, gotErr := scanDatas(test.given)

		if test.wantClosed && !test.given.Closed {
			t.Errorf("TEST[%s] scanDatas(..) did not close the rows struct after an error", test.name)
		}

		if test.wantErr {
			if gotErr == nil {
				t.Errorf("TEST[%s] scanDatas(..) did not return with error when one was expected", test.name)
			}
			continue
		}

		if gotErr != nil {
			t.Errorf("TEST[%s] scanDatas(..) returned with unexpected error:\n%s", test.name, gotErr)
			continue
		}

		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("TEST[%s] scanDatas(..) got:\n%+v\n expected:\n%+v", test.name, got, test.want)
		}
	}
}
