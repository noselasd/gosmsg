// Package gosmsg provides types and functions for working with SMSG (Structured Message)
// format, a tag-based binary protocol used in Utel systems.
//
// The package supports:
//   - Low-level SMSG message construction and parsing (RawSMsg, Tag, Iter)
//   - Schema-based type-safe encoding and decoding (Schema, SchemaDecoder, SchemaEncoder)
//   - YAML-based schema definitions with support for complex types
//
// Basic types supported: bool, int8/16/32/64, float, double, string, binary, timestamps, enums
// Complex types supported: arrays, maps, and nested records
package gosmsg

import (
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// DataType represents the type of a field in an SMSG schema.
// Each DataType corresponds to a specific data encoding and decoding strategy.
type DataType int

const (
	BoolType DataType = iota + 1
	Int8Type
	Int16Type
	Int32Type
	Int64Type
	StringType
	_
	FloatType
	DoubleType
	RecordType
	BinaryType
	TimestampMsType
	TimestampUsType
	EnumType
	ArrayType
	MapType
)

var dataTypeNames = map[DataType]string{
	BoolType:        "bool",
	Int8Type:        "int8",
	Int16Type:       "int16",
	Int32Type:       "int32",
	Int64Type:       "int64",
	StringType:      "string",
	FloatType:       "float",
	DoubleType:      "double",
	RecordType:      "record",
	BinaryType:      "binary",
	TimestampMsType: "timestamp_ms",
	TimestampUsType: "timestamp_us",
	EnumType:        "enum",
	ArrayType:       "array",
	MapType:         "map",
}

var dataTypeMap = map[string]DataType{
	"bool":         BoolType,
	"int8":         Int8Type,
	"int16":        Int16Type,
	"int32":        Int32Type,
	"int64":        Int64Type,
	"string":       StringType,
	"float":        FloatType,
	"double":       DoubleType,
	"timestamp_ms": TimestampMsType,
	"timestamp_us": TimestampUsType,
	"enum":         EnumType,
	"array":        ArrayType,
	"map":          MapType,
	"record":       RecordType,
}

// String returns the string representation of a DataType.
// Returns "DataType(N)" for unknown types.
func (dt DataType) String() string {
	if name, ok := dataTypeNames[dt]; ok {
		return name
	}
	return fmt.Sprintf("DataType(%d)", dt)
}

// namesRegex defines allowed field names (for Avro compatibility).
// Field names must start with a letter or underscore, followed by
// letters, digits, or underscores.
var namesRegex = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// ValidName checks if a name is valid according to schema naming rules.
// Valid names must match the pattern: [A-Za-z_][A-Za-z0-9_]*
// This ensures compatibility with Avro and other schema systems.
func ValidName(name string) bool {
	return namesRegex.MatchString(name)
}

// ToDataType converts a string to a DataType.
// Returns an error if the string does not match any known data type.
// Supported type strings: "bool", "int8", "int16", "int32", "int64",
// "string", "float", "double", "timestamp_ms", "timestamp_us",
// "enum", "array", "map", "record"
func ToDataType(val string) (DataType, error) {
	if dtype, ok := dataTypeMap[val]; ok {
		return dtype, nil
	}
	return 0, fmt.Errorf("%s is an invalid datatype", val)
}

// Field represents a field in a schema using a discriminated union pattern.
// The Type field determines which type-specific fields are valid:
//   - For ArrayType and MapType: ValueType is populated
//   - For RecordType: Fields is populated
//   - For simple types: neither is used
type Field struct {
	Name     string
	Type     DataType
	Nullable bool
	Metadata map[string]any

	// Type-specific fields (discriminated by Type)
	ValueType *Field  // Value type for ArrayType and MapType
	Fields    []Field // Sub fields in a RecordType
}

// String returns a string representation of the field for debugging.
// The format includes the field name, type, and nullability status.
// For complex types (array, map, record), additional structure info is included.
func (f *Field) String() string {
	nullable := "nullable"
	if !f.Nullable {
		nullable = "not-nullable"
	}

	switch f.Type {
	case ArrayType:
		if f.ValueType != nil {
			return fmt.Sprintf("%s: array<%s> (%s)", f.Name, f.ValueType.Type, nullable)
		}
		return fmt.Sprintf("%s: array (%s)", f.Name, nullable)
	case MapType:
		if f.ValueType != nil {
			return fmt.Sprintf("%s: map[string]%s (%s)", f.Name, f.ValueType.Type, nullable)
		}
		return fmt.Sprintf("%s: map (%s)", f.Name, nullable)
	case RecordType:
		return fmt.Sprintf("%s: record<%d fields> (%s)", f.Name, len(f.Fields), nullable)
	default:
		return fmt.Sprintf("%s: %s (%s)", f.Name, f.Type, nullable)
	}
}

// GetSubField returns a nested field by name from a RecordType field.
// Returns an error if this field is not a RecordType or if the named field
// is not found within the record.
//
// This method is only valid for fields of type RecordType.
func (f *Field) GetSubField(name string) (*Field, error) {
	if f.Type != RecordType {
		return nil, fmt.Errorf("GetSubField only valid for RecordType, got %s", f.Type)
	}
	for i := range f.Fields {
		if f.Fields[i].Name == name {
			return &f.Fields[i], nil
		}
	}
	return nil, fmt.Errorf("field '%s' not found in record", name)
}

// NewField creates a new field with validation.
// If metadata is nil, an empty metadata map will be used.
// For certain field types, metadata is required:
//   - EnumType: must have "enum_values" key with []string values
//   - ArrayType/MapType: must have "value_type" key defining element/value type
//   - RecordType (nested): must have "fields" key with nested field definitions
//   - All field types used in decoding: must have "smsg_tag" key with uint16 tag number
//
// Field names must match the pattern [A-Za-z_][A-Za-z0-9_]* except for RecordType
// which has relaxed naming rules for pysmsg compatibility.
func NewField(name string, dtype DataType, nullable bool, metadata map[string]any) (*Field, error) {
	// Validate name (except for RecordType which has relaxed rules for pysmsg compatibility
	if dtype != RecordType && !ValidName(name) {
		return nil, fmt.Errorf("%s is an invalid field name", name)
	}

	if metadata == nil {
		metadata = make(map[string]any)
	}

	field := &Field{
		Name:     name,
		Type:     dtype,
		Nullable: nullable,
		Metadata: metadata,
	}

	// Handle type-specific initialization
	switch dtype {
	case EnumType:
		if err := validateEnumMetadata(metadata); err != nil {
			return nil, err
		}

	case ArrayType, MapType:
		suffix := "element"
		if dtype == MapType {
			suffix = "value"
		}
		valueType, err := buildValueType(name, metadata, suffix)
		if err != nil {
			return nil, err
		}
		field.ValueType = valueType

	case RecordType:
		// Only build nested fields if fields metadata is present (for nested records)
		// Top-level record types don't have fields in metadata
		if _, hasFields := metadata["fields"]; hasFields {
			fields, err := buildRecordFields(metadata)
			if err != nil {
				return nil, err
			}
			field.Fields = fields
		}
	}

	return field, nil
}

// Schema represents a schema definition for SMSG records.
// A schema consists of a record type definition, a list of fields,
// and a version number. Schemas are typically loaded from YAML files
// and used for type-safe encoding and decoding of SMSG messages.
type Schema struct {
	RecordType *Field  // The root record type definition (must be RecordType, non-nullable)
	Fields     []Field // Top-level fields in the schema
	Version    int     // Schema version number
}

// NewSchema creates a new schema with validation.
// The recordType must be a non-nullable RecordType field.
// Returns an error if validation fails.
func NewSchema(recordType *Field, fields []Field, version int) (*Schema, error) {
	if recordType.Type != RecordType {
		return nil, errors.New("record_type Field must have Type=RecordType")
	}
	if recordType.Nullable {
		return nil, errors.New("record_type Field cannot be nullable")
	}
	return &Schema{
		RecordType: recordType,
		Fields:     fields,
		Version:    version,
	}, nil
}

// String returns a string representation of the schema for debugging.
// Includes the record type name, version, and a list of all fields.
func (s *Schema) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Schema{RecordType: %s, Version: %d, Fields: [\n", s.RecordType.Name, s.Version))
	for _, field := range s.Fields {
		sb.WriteString(fmt.Sprintf("  %s\n", field.String()))
	}
	sb.WriteString("]}")
	return sb.String()
}

// GetField returns a field by name from the schema's top-level fields.
// Returns an error if no field with the given name exists.
func (s *Schema) GetField(name string) (*Field, error) {
	for i := range s.Fields {
		if s.Fields[i].Name == name {
			return &s.Fields[i], nil
		}
	}
	return nil, fmt.Errorf("field '%s' not found in schema", name)
}

// SetField adds or updates a field in the schema.
// If a field with the same name already exists, it is replaced.
// Otherwise, the field is appended to the schema's field list.
func (s *Schema) SetField(field Field) {
	for i := range s.Fields {
		if s.Fields[i].Name == field.Name {
			s.Fields[i] = field
			return
		}
	}
	s.Fields = append(s.Fields, field)
}

// Contains checks if a field with the given name exists in the schema's
// top-level fields. Returns true if found, false otherwise.
func (s *Schema) Contains(fieldName string) bool {
	for i := range s.Fields {
		if s.Fields[i].Name == fieldName {
			return true
		}
	}
	return false
}

// validateEnumMetadata validates enum field metadata
func validateEnumMetadata(metadata map[string]any) error {
	enumValuesRaw, ok := metadata["enum_values"]
	if !ok {
		return errors.New("enum_values metadata is required for enum fields")
	}

	enumValues, ok := enumValuesRaw.([]any)
	if !ok || len(enumValues) == 0 {
		return errors.New("enum_values metadata is required for enum fields")
	}

	seen := make(map[string]bool)
	for _, val := range enumValues {
		strVal, ok := val.(string)
		if !ok {
			return errors.New("enum values must be strings")
		}
		if !ValidName(strVal) {
			return fmt.Errorf("%s is an invalid enum value", strVal)
		}
		if seen[strVal] {
			return errors.New("enum_values must be unique")
		}
		seen[strVal] = true
	}

	return nil
}

// buildValueType builds a value type field for array/map fields
func buildValueType(parentName string, metadata map[string]any, suffix string) (*Field, error) {
	valueTypeRaw, ok := metadata["value_type"]
	if !ok {
		return nil, errors.New("value_type metadata is required")
	}

	switch vt := valueTypeRaw.(type) {
	case map[string]any:
		if _, hasName := vt["name"]; !hasName {
			vt["name"] = fmt.Sprintf("%s_%s", parentName, suffix)
		}
		return buildField(vt)
	case string:
		fieldMap := map[string]any{
			"name":     fmt.Sprintf("%s_%s", parentName, suffix),
			"type":     vt,
			"nullable": true,
		}
		return buildField(fieldMap)
	default:
		return nil, errors.New("value_type must be a string or map")
	}
}

// buildRecordFields builds the fields list for a record field
func buildRecordFields(metadata map[string]any) ([]Field, error) {
	fieldsList, ok := metadata["fields"]
	if !ok {
		return nil, errors.New("fields metadata is required for record fields")
	}

	fieldMaps, ok := fieldsList.([]any)
	if !ok {
		return nil, errors.New("fields metadata must be a list")
	}

	fields := make([]Field, 0, len(fieldMaps))
	for _, fieldMap := range fieldMaps {
		fm, ok := fieldMap.(map[string]any)
		if !ok {
			return nil, errors.New("each field must be a map")
		}
		if _, ok := fm["name"]; !ok {
			return nil, errors.New("name is required for record fields")
		}
		field, err := buildField(fm)
		if err != nil {
			return nil, err
		}
		fields = append(fields, *field)
	}

	return fields, nil
}

// buildField builds a field from a map representation
func buildField(mapping map[string]any) (*Field, error) {
	// Validate required attributes
	name, ok := mapping["name"].(string)
	if !ok {
		return nil, errors.New("name is required for fields and must be a string")
	}

	typeStr, ok := mapping["type"].(string)
	if !ok {
		return nil, errors.New("type is required for fields and must be a string")
	}

	nullable, ok := mapping["nullable"].(bool)
	if !ok {
		return nil, errors.New("nullable is required for fields and must be a bool")
	}

	dtype, err := ToDataType(typeStr)
	if err != nil {
		return nil, err
	}

	metadata, _ := mapping["metadata"].(map[string]any)
	if metadata == nil {
		metadata = make(map[string]any)
	}

	return NewField(name, dtype, nullable, metadata)
}

// buildSchema builds a schema from a map representation
func buildSchema(mapping map[string]any) (*Schema, error) {
	recordTypeName, ok := mapping["recordtype"].(string)
	if !ok {
		recordTypeName = "unknown"
	}

	metadata, _ := mapping["metadata"].(map[string]any)
	if metadata == nil {
		metadata = make(map[string]any)
	}

	recordType, err := NewField(recordTypeName, RecordType, false, metadata)
	if err != nil {
		return nil, err
	}

	fieldsRaw, ok := mapping["fields"]
	if !ok {
		return nil, errors.New("fields is required")
	}

	fieldsList, ok := fieldsRaw.([]any)
	if !ok {
		return nil, errors.New("fields must be a list")
	}

	fields := make([]Field, 0, len(fieldsList))
	seen := make(map[string]bool)

	for _, fieldRaw := range fieldsList {
		fieldMap, ok := fieldRaw.(map[string]any)
		if !ok {
			return nil, errors.New("each field must be a map")
		}

		field, err := buildField(fieldMap)
		if err != nil {
			return nil, err
		}

		if seen[field.Name] {
			return nil, fmt.Errorf("%s is defined multiple times", field.Name)
		}
		seen[field.Name] = true
		fields = append(fields, *field)
	}

	version := int(0)
	if v, ok := mapping["version"]; ok {
		// YAML decoder will parse integers as int
		vInt, ok := v.(int)
		if !ok {
			return nil, errors.New("version must be an integer")
		}

		version = vInt
	}

	return NewSchema(recordType, fields, version)
}

// LoadSchemaFromReader loads a schema definition from a YAML stream.
// The YAML format should include:
//   - recordtype: the name of the record type
//   - version: schema version number (optional, defaults to 0)
//   - metadata: metadata map (must include "smsg_tag" for the record type)
//   - fields: list of field definitions
//
// Returns an error if the YAML is invalid or schema validation fails.
func LoadSchemaFromReader(r io.Reader) (*Schema, error) {
	decoder := yaml.NewDecoder(r)
	var mapping map[string]any
	if err := decoder.Decode(&mapping); err != nil {
		return nil, err
	}
	return buildSchema(mapping)
}

// LoadSchema loads a schema definition from a YAML file.
// This is a convenience wrapper around LoadSchemaFromReader that
// handles file opening and closing.
//
// Returns an error if the file cannot be opened or if schema loading fails.
func LoadSchema(filename string) (*Schema, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	return LoadSchemaFromReader(file)
}
