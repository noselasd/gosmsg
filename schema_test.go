package gosmsg

import (
	"strings"
	"testing"
)

// Helper to create schema from YAML string
func schemaFromYAML(yaml string) (*Schema, error) {
	return LoadSchemaFromReader(strings.NewReader(yaml))
}

func TestFieldConstruction(t *testing.T) {
	// Valid field
	field, err := NewField("test_field", StringType, true, nil)
	if err != nil {
		t.Fatalf("NewField failed: %v", err)
	}
	if field.Name != "test_field" || field.Type != StringType || !field.Nullable {
		t.Errorf("Field properties incorrect")
	}

	// Invalid field name
	_, err = NewField("invalid name", StringType, false, nil)
	if err == nil {
		t.Error("Expected error for invalid field name")
	}

	// Enum without enum_values
	_, err = NewField("app", EnumType, false, nil)
	if err == nil {
		t.Error("Expected error for enum without enum_values")
	}

	// Valid enum
	metadata := map[string]interface{}{
		"enum_values": []interface{}{"CAP", "MAP", "INAP"},
	}
	_, err = NewField("app", EnumType, false, metadata)
	if err != nil {
		t.Errorf("Valid enum failed: %v", err)
	}
}

func TestSchemaConstruction(t *testing.T) {
	recordType, err := NewField("test", RecordType, false, map[string]interface{}{
		"smsg_tag": 0x1234,
	})
	if err != nil {
		t.Fatalf("NewField for record type failed: %v", err)
	}

	field1, _ := NewField("name", StringType, false, nil)
	field2, _ := NewField("age", Int32Type, true, nil)

	schema, err := NewSchema(recordType, []Field{*field1, *field2}, 1)
	if err != nil {
		t.Fatalf("NewSchema failed: %v", err)
	}

	if schema.Version != 1 {
		t.Errorf("Version = %d, want 1", schema.Version)
	}
	if len(schema.Fields) != 2 {
		t.Errorf("Fields count = %d, want 2", len(schema.Fields))
	}

	// Record type must not be nullable
	recordType.Nullable = true
	_, err = NewSchema(recordType, []Field{*field1}, 0)
	if err == nil {
		t.Error("Expected error for nullable record type")
	}
}

func TestLoadSchema(t *testing.T) {
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
`
	schema, err := schemaFromYAML(yaml)
	if err != nil {
		t.Fatalf("Load schema failed: %v", err)
	}

	if schema.RecordType.Name != "sip" {
		t.Errorf("RecordType name = %s, want sip", schema.RecordType.Name)
	}
	if schema.Version != 1 {
		t.Errorf("Version = %d, want 1", schema.Version)
	}
	if len(schema.Fields) != 2 {
		t.Fatalf("Fields count = %d, want 2", len(schema.Fields))
	}
	if schema.Fields[0].Name != "start_ts" || schema.Fields[0].Type != Int64Type {
		t.Error("First field incorrect")
	}
}

func TestSchemaValidation(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name: "missing nullable field",
			yaml: `
recordtype: test
fields:
- name: field1
  type: string
`,
			wantErr: "nullable is required",
		},
		{
			name: "invalid data type",
			yaml: `
recordtype: test
fields:
- name: field1
  nullable: false
  type: invalidtype
`,
			wantErr: "invalid datatype",
		},
		{
			name: "duplicate field names",
			yaml: `
recordtype: test
fields:
- name: field1
  nullable: false
  type: string
- name: field1
  nullable: true
  type: string
`,
			wantErr: "defined multiple times",
		},
		{
			name: "invalid field name",
			yaml: `
recordtype: test
fields:
- name: "invalid name"
  nullable: false
  type: string
`,
			wantErr: "invalid field name",
		},
		{
			name: "wrong nullable type",
			yaml: `
recordtype: test
fields:
- name: field1
  nullable: "false"
  type: string
`,
			wantErr: "nullable is required",
		},
		{
			name: "invalid version type",
			yaml: `
recordtype: test
version: "not_a_number"
fields:
- name: field1
  nullable: false
  type: string
`,
			wantErr: "version must be an integer",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := schemaFromYAML(tt.yaml)
			if err == nil {
				t.Fatal("Expected error but got none")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Error = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestEnumValidation(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name: "missing enum_values",
			yaml: `
recordtype: test
fields:
- name: app
  nullable: false
  type: enum
`,
			wantErr: "enum_values metadata is required",
		},
		{
			name: "invalid enum value",
			yaml: `
recordtype: test
fields:
- name: app
  nullable: false
  type: enum
  metadata:
    enum_values: ["CAP", "MAP", "IN-AP"]
`,
			wantErr: "invalid enum value",
		},
		{
			name: "duplicate enum values",
			yaml: `
recordtype: test
fields:
- name: app
  nullable: false
  type: enum
  metadata:
    enum_values: ["CAP", "MAP", "MAP"]
`,
			wantErr: "enum_values must be unique",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := schemaFromYAML(tt.yaml)
			if err == nil {
				t.Fatal("Expected error but got none")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Error = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}

	// Valid enum
	yaml := `
recordtype: test
fields:
- name: app
  nullable: false
  type: enum
  metadata:
    enum_values: ["CAP", "MAP", "INAP"]
`
	schema, err := schemaFromYAML(yaml)
	if err != nil {
		t.Fatalf("Valid enum failed: %v", err)
	}
	if schema.Fields[0].Type != EnumType {
		t.Error("Field type should be EnumType")
	}
}

func TestArrayFields(t *testing.T) {
	// Simple array
	yaml := `
recordtype: test
fields:
- name: numbers
  nullable: false
  type: array
  metadata:
    value_type: int32
`
	schema, err := schemaFromYAML(yaml)
	if err != nil {
		t.Fatalf("Simple array failed: %v", err)
	}
	field := schema.Fields[0]
	if field.Type != ArrayType {
		t.Error("Field should be ArrayType")
	}
	if field.ValueType == nil || field.ValueType.Type != Int32Type {
		t.Error("Array value type should be int32")
	}

	// Nested array
	yaml = `
recordtype: test
fields:
- name: matrix
  nullable: false
  type: array
  metadata:
    value_type:
      type: array
      nullable: true
      metadata:
        value_type: float
`
	schema, err = schemaFromYAML(yaml)
	if err != nil {
		t.Fatalf("Nested array failed: %v", err)
	}
	field = schema.Fields[0]
	if field.Type != ArrayType {
		t.Error("Outer field should be ArrayType")
	}
	if field.ValueType.Type != ArrayType {
		t.Error("Inner field should be ArrayType")
	}
	if field.ValueType.ValueType.Type != FloatType {
		t.Error("Innermost type should be float")
	}

	// Missing value_type
	yaml = `
recordtype: test
fields:
- name: numbers
  nullable: false
  type: array
`
	_, err = schemaFromYAML(yaml)
	if err == nil || !strings.Contains(err.Error(), "value_type metadata is required") {
		t.Error("Expected error for missing value_type")
	}
}

func TestMapFields(t *testing.T) {
	// Simple map with string shorthand
	yaml := `
recordtype: test
fields:
- name: headers
  nullable: true
  type: map
  metadata:
    value_type: string
`
	schema, err := schemaFromYAML(yaml)
	if err != nil {
		t.Fatalf("Simple map failed: %v", err)
	}
	field := schema.Fields[0]
	if field.Type != MapType {
		t.Error("Field should be MapType")
	}
	if field.ValueType == nil || field.ValueType.Type != StringType {
		t.Error("Map value type should be string")
	}

	// Map with explicit value type
	yaml = `
recordtype: test
fields:
- name: counters
  nullable: false
  type: map
  metadata:
    value_type:
      type: int64
      nullable: false
`
	schema, err = schemaFromYAML(yaml)
	if err != nil {
		t.Fatalf("Map with explicit type failed: %v", err)
	}
	field = schema.Fields[0]
	if field.ValueType.Type != Int64Type || field.ValueType.Nullable {
		t.Error("Map value type incorrect")
	}

	// Missing value_type
	yaml = `
recordtype: test
fields:
- name: data
  nullable: false
  type: map
`
	_, err = schemaFromYAML(yaml)
	if err == nil || !strings.Contains(err.Error(), "value_type metadata is required") {
		t.Error("Expected error for missing value_type")
	}
}

func TestRecordFields(t *testing.T) {
	yaml := `
recordtype: test
fields:
- name: destination
  nullable: false
  type: record
  metadata:
    fields:
    - name: country
      type: string
      nullable: false
    - name: operator
      type: string
      nullable: true
`
	schema, err := schemaFromYAML(yaml)
	if err != nil {
		t.Fatalf("Nested record failed: %v", err)
	}

	field := schema.Fields[0]
	if field.Type != RecordType {
		t.Error("Field should be RecordType")
	}
	if len(field.Fields) != 2 {
		t.Fatalf("Record should have 2 fields, got %d", len(field.Fields))
	}
	if field.Fields[0].Name != "country" || field.Fields[0].Type != StringType {
		t.Error("First nested field incorrect")
	}
	if field.Fields[1].Name != "operator" || !field.Fields[1].Nullable {
		t.Error("Second nested field incorrect")
	}

	// Test GetSubField
	subField, err := field.GetSubField("country")
	if err != nil {
		t.Errorf("GetSubField failed: %v", err)
	}
	if subField.Name != "country" {
		t.Error("GetSubField returned wrong field")
	}

	// GetSubField on non-record
	stringField, _ := NewField("test", StringType, false, nil)
	_, err = stringField.GetSubField("anything")
	if err == nil {
		t.Error("GetSubField should fail on non-RecordType")
	}
}

func TestSchemaOperations(t *testing.T) {
	recordType, _ := NewField("test", RecordType, false, nil)
	field1, _ := NewField("name", StringType, false, nil)
	field2, _ := NewField("age", Int32Type, true, nil)
	schema, _ := NewSchema(recordType, []Field{*field1, *field2}, 0)

	// GetField
	field, err := schema.GetField("name")
	if err != nil {
		t.Errorf("GetField failed: %v", err)
	}
	if field.Name != "name" {
		t.Error("GetField returned wrong field")
	}

	_, err = schema.GetField("nonexistent")
	if err == nil {
		t.Error("GetField should fail for nonexistent field")
	}

	// Contains
	if !schema.Contains("name") {
		t.Error("Contains should return true for existing field")
	}
	if schema.Contains("nonexistent") {
		t.Error("Contains should return false for nonexistent field")
	}

	// SetField - update existing
	updatedField, _ := NewField("name", StringType, true, nil)
	schema.SetField(*updatedField)
	if len(schema.Fields) != 2 {
		t.Error("SetField should not add duplicate")
	}
	field, _ = schema.GetField("name")
	if !field.Nullable {
		t.Error("Field should be updated")
	}

	// SetField - add new
	newField, _ := NewField("email", StringType, false, nil)
	schema.SetField(*newField)
	if len(schema.Fields) != 3 {
		t.Error("SetField should add new field")
	}
	if !schema.Contains("email") {
		t.Error("New field should exist")
	}
}

func TestVersionHandling(t *testing.T) {
	// Valid version
	yaml := `
recordtype: test
version: 42
fields:
- name: field1
  nullable: false
  type: string
`
	schema, err := schemaFromYAML(yaml)
	if err != nil {
		t.Fatalf("Valid version failed: %v", err)
	}
	if schema.Version != 42 {
		t.Errorf("Version = %d, want 42", schema.Version)
	}

	// Version 0 (default when not specified)
	yaml = `
recordtype: test
fields:
- name: field1
  nullable: false
  type: string
`
	schema, err = schemaFromYAML(yaml)
	if err != nil {
		t.Fatalf("Default version failed: %v", err)
	}
	if schema.Version != 0 {
		t.Errorf("Version = %d, want 0", schema.Version)
	}
}

func TestDataTypeString(t *testing.T) {
	tests := []struct {
		dtype DataType
		want  string
	}{
		{BoolType, "bool"},
		{Int32Type, "int32"},
		{StringType, "string"},
		{ArrayType, "array"},
		{MapType, "map"},
		{RecordType, "record"},
		{EnumType, "enum"},
	}

	for _, tt := range tests {
		if got := tt.dtype.String(); got != tt.want {
			t.Errorf("%v.String() = %q, want %q", tt.dtype, got, tt.want)
		}
	}
}

func TestValidName(t *testing.T) {
	tests := []struct {
		name  string
		valid bool
	}{
		{"valid_name", true},
		{"ValidName", true},
		{"name123", true},
		{"_name", true},
		{"123invalid", false},
		{"invalid-name", false},
		{"invalid name", false},
		{"", false},
	}

	for _, tt := range tests {
		if got := ValidName(tt.name); got != tt.valid {
			t.Errorf("ValidName(%q) = %v, want %v", tt.name, got, tt.valid)
		}
	}
}

func TestFieldString(t *testing.T) {
	// Simple field
	field, _ := NewField("name", StringType, false, nil)
	s := field.String()
	if !strings.Contains(s, "name") || !strings.Contains(s, "string") {
		t.Errorf("Field.String() = %q, want to contain name and type", s)
	}

	// Array field
	arrayField, _ := NewField("items", ArrayType, true, map[string]interface{}{
		"value_type": "int32",
	})
	s = arrayField.String()
	if !strings.Contains(s, "array") {
		t.Errorf("Array field String should contain 'array', got %q", s)
	}
}
