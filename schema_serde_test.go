package gosmsg

import (
	"errors"
	"fmt"
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

// ============================================================================
// Edge Case Tests - Demonstrating Bugs
// ============================================================================

// TestCoerceBoolEmptyValue tests that empty tags are handled correctly for bool fields.
// Empty non-nullable bool should return an error from coerce() before reaching coerceToBool().
func TestCoerceBoolEmptyValue(t *testing.T) {
	tests := []struct {
		name      string
		nullable  bool
		expectNil bool
		expectErr bool
	}{
		{
			name:      "non_nullable_empty",
			nullable:  false,
			expectNil: false,
			expectErr: true,
		},
		{
			name:      "nullable_empty",
			nullable:  true,
			expectNil: true,
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nullableStr := "false"
			if tt.nullable {
				nullableStr = "true"
			}

			boolSchema := fmt.Sprintf(`
recordtype: test
version: 1
metadata:
    smsg_tag: 0x1000
fields:
- name: flag
  nullable: %s
  type: bool
  metadata:
    smsg_tag: 0x1001
`, nullableStr)

			s, err := LoadSchemaFromReader(strings.NewReader(boolSchema))
			if err != nil {
				t.Fatal(err)
			}

			// Message with empty boolean value (tag 0x1001 with length 0)
			r := RawSMsg{[]byte("9000 10010 00000 ")}

			decoder, err := NewSchemaDecoder([]Schema{*s})
			if err != nil {
				t.Fatal(err)
			}

			decoded, err := decoder.Decode(r)
			if tt.expectErr {
				if err == nil {
					t.Error("Expected error for empty non-nullable bool, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			flagValue := decoded.Fields["flag"]
			if tt.expectNil {
				if flagValue != nil {
					t.Errorf("Expected nil for nullable empty bool, got %v", flagValue)
				}
			} else {
				if flagValue == nil {
					t.Error("Expected non-nil value")
				}
			}
		})
	}
}

// TestEmptyTagHandling comprehensively tests how empty tags are handled for all field types.
// Empty tags (length 0) should be treated as:
// - Empty string for string fields (nullable or not)
// - nil for nullable non-string fields
// - Error for non-nullable non-string fields
func TestEmptyTagHandling(t *testing.T) {
	tests := []struct {
		name         string
		fieldType    string
		nullable     bool
		expectValue  any // Expected value (nil, "", etc.)
		expectError  bool
		enumMetadata string // Additional metadata for enum types
	}{
		// String fields: empty tag -> "" regardless of nullable
		{name: "string_nullable", fieldType: "string", nullable: true, expectValue: "", expectError: false},
		{name: "string_non_nullable", fieldType: "string", nullable: false, expectValue: "", expectError: false},

		// Nullable non-string fields: empty tag -> nil
		{name: "int_nullable", fieldType: "int64", nullable: true, expectValue: nil, expectError: false},
		{name: "bool_nullable", fieldType: "bool", nullable: true, expectValue: nil, expectError: false},
		{name: "float_nullable", fieldType: "float", nullable: true, expectValue: nil, expectError: false},
		{name: "double_nullable", fieldType: "double", nullable: true, expectValue: nil, expectError: false},
		// Note: binary type is not in dataTypeMap, so can't be loaded from YAML schemas
		{
			name:         "enum_nullable",
			fieldType:    "enum",
			nullable:     true,
			expectValue:  nil,
			expectError:  false,
			enumMetadata: "\n    enum_values: [\"A\", \"B\", \"C\"]",
		},

		// Non-nullable non-string fields: empty tag -> error
		{name: "int_non_nullable", fieldType: "int64", nullable: false, expectValue: nil, expectError: true},
		{name: "bool_non_nullable", fieldType: "bool", nullable: false, expectValue: nil, expectError: true},
		{name: "float_non_nullable", fieldType: "float", nullable: false, expectValue: nil, expectError: true},
		{name: "double_non_nullable", fieldType: "double", nullable: false, expectValue: nil, expectError: true},
		// Note: binary type is not in dataTypeMap, so can't be loaded from YAML schemas
		{
			name:         "enum_non_nullable",
			fieldType:    "enum",
			nullable:     false,
			expectValue:  nil,
			expectError:  true,
			enumMetadata: "\n    enum_values: [\"A\", \"B\", \"C\"]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nullableStr := "false"
			if tt.nullable {
				nullableStr = "true"
			}

			schemaYAML := fmt.Sprintf(`
recordtype: test
version: 1
metadata:
    smsg_tag: 0x1000
fields:
- name: testfield
  nullable: %s
  type: %s
  metadata:
    smsg_tag: 0x1001%s
`, nullableStr, tt.fieldType, tt.enumMetadata)

			s, err := LoadSchemaFromReader(strings.NewReader(schemaYAML))
			if err != nil {
				t.Fatalf("Failed to load schema: %v", err)
			}

			// Message with empty field value (tag 0x1001 with length 0)
			r := RawSMsg{[]byte("9000 10010 00000 ")}

			decoder, err := NewSchemaDecoder([]Schema{*s})
			if err != nil {
				t.Fatalf("Failed to create decoder: %v", err)
			}

			decoded, err := decoder.Decode(r)
			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for empty non-nullable %s, got nil", tt.fieldType)
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			fieldValue := decoded.Fields["testfield"]
			if tt.expectValue == nil {
				if fieldValue != nil {
					t.Errorf("Expected nil, got %v (%T)", fieldValue, fieldValue)
				}
			} else {
				if fieldValue != tt.expectValue {
					t.Errorf("Expected %v (%T), got %v (%T)", tt.expectValue, tt.expectValue, fieldValue, fieldValue)
				}
			}
		})
	}
}
