package gosmsg

import (
	"log"
	"maps"
	"strings"
	"testing"
)

func TestSchemaDecode(t *testing.T) {

	yaml := `
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
	s, err := LoadSchemaFromReader(strings.NewReader(yaml))
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
