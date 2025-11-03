package gosmsg

import (
	"errors"
	"log"
	"maps"
	"strings"
	"testing"
)

var schema string = `
recordtype: sip
version: 1
metadata:
    description: "Test schema"
    smsg_tag: 0x1019
fields:
- name: start_ts
  nullable: false
  type: int64
  metadata:
    smsg_tag: 0x1020
- name: anr
  nullable: true
  type: string
  metadata:
    smsg_tag: 0x1033
`

func TestSchemaDecode(t *testing.T) {

	s, err := LoadSchemaFromReader(strings.NewReader(schema))
	if err != nil {
		log.Fatal(err)
	}

	r := RawSMsg{[]byte("9019 10204 123410333 98700000 ")}
	sd, err := NewSchemaDecoder([]Schema{*s})
	if err != nil {
		t.Fatal(err)
	}
	d, err := sd.Decode(r)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]any{
		"anr":      "987",
		"start_ts": int64(1234),
	}

	if !maps.Equal(expected, d) {
		t.Errorf("Got %+v, expected %+v\n", d, expected)
	}
}

func TestSchemaDecodeConversionErr(t *testing.T) {

	s, err := LoadSchemaFromReader(strings.NewReader(schema))
	if err != nil {
		log.Fatal(err)
	}

	r := RawSMsg{[]byte("9019 10204 A23410333 98700000 ")}
	sd, err := NewSchemaDecoder([]Schema{*s})
	if err != nil {
		t.Fatal(err)
	}
	_, err = sd.Decode(r)
	if err == nil {
		t.Fatal(err)
	} else if !strings.Contains(err.Error(), "start_ts") {
		t.Fatal(err)
	}
}

func TestSchemaDecodeMissingSchema(t *testing.T) {

	s, err := LoadSchemaFromReader(strings.NewReader(schema))
	if err != nil {
		log.Fatal(err)
	}

	r := RawSMsg{[]byte("9020 10204 123410333 98700000 ")}
	sd, err := NewSchemaDecoder([]Schema{*s})
	if err != nil {
		t.Fatal(err)
	}
	_, err = sd.Decode(r)
	if err == nil {
		t.Fatal(err)
	} else {
		var e *MissingSchemaError
		if !errors.As(err, &e) {
			t.Fatal(err)
		}
		if e.Tag != 0x1020 {
			t.Fatal(err)

		}
	}
}
