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

// ============================================================================
// Decoder Tests
// ============================================================================

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

	// Verify record type info
	if d.RecordType != "sip" {
		t.Errorf("RecordType = %s, want sip", d.RecordType)
	}
	if d.RecordTag != 0x1019 {
		t.Errorf("RecordTag = 0x%04X, want 0x1019", d.RecordTag)
	}

	// Verify fields
	expected := map[string]any{
		"anr":      "987",
		"start_ts": int64(1234),
	}

	if !maps.Equal(expected, d.Fields) {
		t.Errorf("Got %+v, expected %+v\n", d.Fields, expected)
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

// ============================================================================
// Encoder Tests
// ============================================================================

func TestSchemaEncode(t *testing.T) {
	s, err := LoadSchemaFromReader(strings.NewReader(schema))
	if err != nil {
		t.Fatal(err)
	}

	encoder, err := NewSchemaEncoder([]Schema{*s})
	if err != nil {
		t.Fatal(err)
	}

	msg := &DecodedMessage{
		RecordType: "sip",
		RecordTag:  0x1019,
		Fields: Fields{
			"start_ts": int64(1234),
			"anr":      "987",
		},
	}

	raw, err := encoder.Encode(msg)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Verify the encoded message
	if string(raw.Data) != "9019 10204 123410333 98700000 \n" {
		t.Errorf("Encoded = %q, want %q", string(raw.Data), "9019 10204 123410333 98700000 \n")
	}
}

func TestSchemaEncodeRoundTrip(t *testing.T) {
	s, err := LoadSchemaFromReader(strings.NewReader(schema))
	if err != nil {
		t.Fatal(err)
	}

	decoder, err := NewSchemaDecoder([]Schema{*s})
	if err != nil {
		t.Fatal(err)
	}

	encoder, err := NewSchemaEncoder([]Schema{*s})
	if err != nil {
		t.Fatal(err)
	}

	// Original message
	original := RawSMsg{[]byte("9019 10204 123410333 98700000 ")}

	// Decode
	decoded, err := decoder.Decode(original)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Encode back
	encoded, err := encoder.Encode(decoded)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Decode again to verify
	decoded2, err := decoder.Decode(*encoded)
	if err != nil {
		t.Fatalf("Second decode failed: %v", err)
	}

	// Compare decoded values
	if decoded.RecordType != decoded2.RecordType {
		t.Errorf("RecordType mismatch: %s vs %s", decoded.RecordType, decoded2.RecordType)
	}
	if decoded.RecordTag != decoded2.RecordTag {
		t.Errorf("RecordTag mismatch: 0x%04X vs 0x%04X", decoded.RecordTag, decoded2.RecordTag)
	}
	if !maps.Equal(decoded.Fields, decoded2.Fields) {
		t.Errorf("Fields mismatch:\nOriginal: %+v\nRound-trip: %+v", decoded.Fields, decoded2.Fields)
	}
}

func TestSchemaEncodeMissingRequired(t *testing.T) {
	s, err := LoadSchemaFromReader(strings.NewReader(schema))
	if err != nil {
		t.Fatal(err)
	}

	encoder, err := NewSchemaEncoder([]Schema{*s})
	if err != nil {
		t.Fatal(err)
	}

	// Missing required field start_ts
	msg := &DecodedMessage{
		RecordType: "sip",
		RecordTag:  0x1019,
		Fields: Fields{
			"anr": "987",
		},
	}

	_, err = encoder.Encode(msg)
	if err == nil {
		t.Fatal("Expected error for missing required field")
	}
	if !strings.Contains(err.Error(), "start_ts") {
		t.Errorf("Error should mention start_ts: %v", err)
	}
}

func TestSchemaEncodeTypeMismatch(t *testing.T) {
	s, err := LoadSchemaFromReader(strings.NewReader(schema))
	if err != nil {
		t.Fatal(err)
	}

	encoder, err := NewSchemaEncoder([]Schema{*s})
	if err != nil {
		t.Fatal(err)
	}

	// Wrong type for start_ts (string instead of int64)
	msg := &DecodedMessage{
		RecordType: "sip",
		RecordTag:  0x1019,
		Fields: Fields{
			"start_ts": "not a number",
			"anr":      "987",
		},
	}

	_, err = encoder.Encode(msg)
	if err == nil {
		t.Fatal("Expected error for type mismatch")
	}
	if !strings.Contains(err.Error(), "expected int64") {
		t.Errorf("Error should mention type mismatch: %v", err)
	}
}

func TestSchemaEncodeNullable(t *testing.T) {
	s, err := LoadSchemaFromReader(strings.NewReader(schema))
	if err != nil {
		t.Fatal(err)
	}

	encoder, err := NewSchemaEncoder([]Schema{*s})
	if err != nil {
		t.Fatal(err)
	}

	decoder, err := NewSchemaDecoder([]Schema{*s})
	if err != nil {
		t.Fatal(err)
	}

	// Omit nullable field anr
	msg := &DecodedMessage{
		RecordType: "sip",
		RecordTag:  0x1019,
		Fields: Fields{
			"start_ts": int64(1234),
		},
	}

	raw, err := encoder.Encode(msg)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Decode and verify anr is nil
	decoded, err := decoder.Decode(*raw)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Fields["anr"] != nil {
		t.Errorf("Expected anr to be nil, got %v", decoded.Fields["anr"])
	}
}

func TestSchemaEncodeNoSchema(t *testing.T) {
	s, err := LoadSchemaFromReader(strings.NewReader(schema))
	if err != nil {
		t.Fatal(err)
	}

	encoder, err := NewSchemaEncoder([]Schema{*s})
	if err != nil {
		t.Fatal(err)
	}

	// Wrong record type
	msg := &DecodedMessage{
		RecordType: "unknown",
		RecordTag:  0x9999,
		Fields:     Fields{},
	}

	_, err = encoder.Encode(msg)
	if err == nil {
		t.Fatal("Expected error for unknown record type")
	}
	if !strings.Contains(err.Error(), "no schema found") {
		t.Errorf("Error should mention missing schema: %v", err)
	}
}
