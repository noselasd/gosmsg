package gosmsg

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	// defaultCapacity is the default pre-allocation size for internal maps and slices.
	// This reduces allocations during decoding by pre-allocating reasonable capacity.
	defaultCapacity = 64
)

// Fields represents the decoded field values in an SMSG message.
// Maps field names to their typed values according to the schema.
//
// Value types depend on the field's DataType:
//   - Int8Type, Int16Type, Int32Type, Int64Type -> int64
//   - FloatType, DoubleType -> float64
//   - BoolType -> bool
//   - StringType, EnumType -> string
//   - BinaryType -> []byte
//   - Nullable fields that are missing or empty -> nil
type Fields map[string]any

// DecodedMessage represents a decoded SMSG message with its record type
// metadata and field values. This structure is returned by SchemaDecoder.Decode
// and can be passed to SchemaEncoder.Encode for round-trip encoding.
//
// The RecordType and RecordTag fields identify the message type, which is
// essential for routing and processing in multi-message systems.
type DecodedMessage struct {
	RecordType string // Name of the record type (e.g., "sip", "call_detail")
	RecordTag  uint16 // Numeric tag identifying the record type (e.g., 0x1019)
	Fields     Fields // Decoded field values keyed by field name
}

// String returns a string representation of the decoded message for debugging.
// Shows the record type name, tag, and field count (but not field values).
func (d *DecodedMessage) String() string {
	return fmt.Sprintf("DecodedMessage{RecordType: %s, RecordTag: 0x%04X, Fields: %d}",
		d.RecordType, d.RecordTag, len(d.Fields))
}

// Function to coerce a SMSG field to schema determined type
type coerceFunc func(field *fieldData, val []byte) (any, error)

// fieldData/schemaCoercion pre-computed conversion help for converting a field

type fieldData struct {
	isNullable bool
	isString   bool
	smsgTag    uint16
	name       string
	enumValues map[string]bool
	coerceFunc coerceFunc
}

type schemaCoercion struct {
	recordTypeName string
	recordTypeTag  uint16
	fields         []fieldData
}

// SchemaDecoder decodes RawSMsg messages into typed DecodedMessage structures
// using registered schemas. The decoder pre-computes type coercion functions
// during initialization for efficient repeated decoding.
//
// A SchemaDecoder can handle multiple message types (schemas) and automatically
// selects the appropriate schema based on the message's record type tag.
//
// Create a SchemaDecoder using NewSchemaDecoder.
type SchemaDecoder struct {
	coercers map[uint16]schemaCoercion // map from record type tag to schemaCoersion
}

// Find smsg_tag for the field
func extractSmsgTag(field *Field) (uint16, error) {
	smsgTag, ok := field.Metadata["smsg_tag"]
	if !ok {
		return 0, fmt.Errorf("%s is missing smsg_tag metadata", field.Name)
	}
	smsgTagInt, ok := smsgTag.(int)
	if !ok {
		return 0, fmt.Errorf("%s smsg_tag metadata must be an uint16", field.Name)
	}
	return uint16(smsgTagInt), nil
}

func coerceToString(_ *fieldData, val []byte) (any, error) {
	return strings.ToValidUTF8(string(val), "?"), nil
}

func coerceToInt8(_ *fieldData, val []byte) (any, error) {
	v, err := strconv.ParseInt(string(val), 10, 8)
	return int8(v), err
}
func coerceToInt16(_ *fieldData, val []byte) (any, error) {
	v, err := strconv.ParseInt(string(val), 10, 16)
	return int16(v), err
}

func coerceToInt32(_ *fieldData, val []byte) (any, error) {
	v, err := strconv.ParseInt(string(val), 10, 32)
	return int32(v), err
}
func coerceToInt64(_ *fieldData, val []byte) (any, error) {
	v, err := strconv.ParseInt(string(val), 10, 64)
	return v, err
}

func coerceToFloat32(_ *fieldData, val []byte) (any, error) {
	v, err := strconv.ParseFloat(string(val), 32)
	return float32(v), err
}

func coerceToFloat64(_ *fieldData, val []byte) (any, error) {
	return strconv.ParseFloat(string(val), 64)
}

func coerceToBool(_ *fieldData, val []byte) (any, error) {
	return val[0] != '0', nil
}

func coerceToEnum(f *fieldData, val []byte) (any, error) {
	s := string(val)
	if _, ok := f.enumValues[s]; !ok {
		return "", fmt.Errorf("invalid enum value %s for %s", s, f.name)
	}
	return s, nil // Guaranteed valid string at this point
}
func coerceToBytes(_ *fieldData, val []byte) (any, error) {
	return val, nil
}
func newFieldData(f *Field) (fieldData, error) {
	smsgTag, err := extractSmsgTag(f)
	if err != nil {
		return fieldData{}, err
	}
	var coerceFunc coerceFunc
	var enumMap map[string]bool
	switch f.Type {
	// We convert all integers to int64, float/double to float64 like pysmsg. This may be a mistake.
	case EnumType:
		enumMap = make(map[string]bool)
		// validateEnumMetadata ensures enum_values is []any containing only strings
		enumValuesRaw := f.Metadata["enum_values"].([]any)
		for _, v := range enumValuesRaw {
			enumMap[v.(string)] = true
		}
		coerceFunc = coerceToEnum
	case Int8Type:
		coerceFunc = coerceToInt8
	case Int16Type:
		coerceFunc = coerceToInt16
	case Int32Type:
		coerceFunc = coerceToInt32
	case Int64Type:
		coerceFunc = coerceToInt64
	case FloatType:
		coerceFunc = coerceToFloat32
	case DoubleType:
		coerceFunc = coerceToFloat64
	case BoolType:
		coerceFunc = coerceToBool
	case BinaryType:
		coerceFunc = coerceToBytes
	case StringType:
		coerceFunc = coerceToString
	default:
		return fieldData{}, fmt.Errorf("type conversion of %s is not implemented", f.Name)
	}

	return fieldData{
		isNullable: f.Nullable,
		isString:   f.Type == StringType,
		smsgTag:    smsgTag,
		name:       f.Name,
		enumValues: enumMap,
		coerceFunc: coerceFunc,
	}, nil
}

func newSchemaCoercion(s *Schema) (schemaCoercion, error) {
	smsgTag, err := extractSmsgTag(s.RecordType)
	if err != nil {
		return schemaCoercion{}, err
	}

	fields := make([]fieldData, len(s.Fields))
	for i := range s.Fields {
		f := &s.Fields[i]
		d, err := newFieldData(f)
		if err != nil {
			return schemaCoercion{}, err
		}
		fields[i] = d
	}

	return schemaCoercion{
		recordTypeName: s.RecordType.Name,
		recordTypeTag:  smsgTag,
		fields:         fields,
	}, nil
}

func (s *SchemaDecoder) coerce(recordType *Tag, tags map[uint16][]byte) (*DecodedMessage, error) {
	sc, ok := s.coercers[recordType.Tag]
	if !ok {
		return nil, &MissingSchemaError{Tag: recordType.Tag}
	}

	// Build message once - will be returned even on error (partial decode)
	msg := &DecodedMessage{
		RecordType: sc.recordTypeName,
		RecordTag:  recordType.Tag,
		Fields:     make(Fields, len(sc.fields)),
	}

	for i := range sc.fields {
		fd := &sc.fields[i]
		rawVal, ok := tags[fd.smsgTag]

		// Handle missing or empty tags
		if !ok || len(rawVal) == 0 {
			// Empty tags (present but zero length) → "" for strings, nil/error for others
			// Missing tags (not present) → nil for nullable, error for non-nullable
			if len(rawVal) == 0 && ok && fd.isString {
				msg.Fields[fd.name] = ""
				continue
			}
			if fd.isNullable {
				msg.Fields[fd.name] = nil
				continue
			}
			return msg, fmt.Errorf("field %s is missing from record, but not nullable", fd.name)
		}

		// Coerce non-empty value
		val, err := fd.coerceFunc(fd, rawVal)
		if err != nil {
			return msg, fmt.Errorf("failed converting %s in %s:%s : %w", rawVal, sc.recordTypeName, fd.name, err)
		}
		msg.Fields[fd.name] = val
	}

	return msg, nil
}

// Decode decodes a RawSMsg into a typed DecodedMessage using the registered schemas.
// Returns the decoded message with record type information and typed field values.
//
// Type conversions:
//   - All integer types (int8/16/32/64) -> int64
//   - All floating point types (float/double) -> float64
//   - Enum values -> string (validated against enum_values)
//   - Binary data -> []byte
//   - Missing nullable fields -> nil
//   - Empty string fields -> ""
//
// Error handling:
//   - Returns MissingSchemaError if no schema matches the message's record type tag
//   - Returns conversion errors for invalid field data
//   - Returns error for missing non-nullable fields
//   - May return a partially decoded message with error (fields decoded before failure)
//
// Check for MissingSchemaError:
//
//	var e *MissingSchemaError
//	if errors.As(err, &e) {
//	    handleMissingSchemaError(err)
//	}
func (s *SchemaDecoder) Decode(r RawSMsg) (*DecodedMessage, error) {
	it := r.Tags()

	recordType, err := it.NextTag()
	if err != nil {
		return nil, err
	}
	tags := make(map[uint16][]byte, defaultCapacity)
	it = recordType.SubTags()
	for t, err := it.NextTag(); err != EOS; t, err = it.NextTag() {
		if err != nil {
			return nil, err
		}
		if t.Tag == 0 { // terminator tag
			break
		}
		tags[t.Tag] = t.Data
	}

	return s.coerce(&recordType, tags)
}

// NewSchemaDecoder creates a SchemaDecoder that can decode SMSG messages
// according to the provided schemas.
//
// The decoder pre-computes type coercion functions for all fields in all schemas,
// enabling efficient repeated decoding. Multiple schemas can be registered to handle
// different message types in the same decoder.
//
// Schema requirements:
//   - Each schema's RecordType must have an "smsg_tag" in its metadata
//   - All fields used in decoding must have an "smsg_tag" in their metadata
//   - Enum fields must have "enum_values" metadata
//   - All schema record type tags must be unique
//
// Returns an error if any schema is invalid or missing required metadata.
func NewSchemaDecoder(schemas []Schema) (*SchemaDecoder, error) {
	coercers := make(map[uint16]schemaCoercion, len(schemas))
	for i := range schemas {
		schema := &schemas[i]
		c, err := newSchemaCoercion(schema)
		if err != nil {
			return nil, err
		}
		coercers[c.recordTypeTag] = c
	}

	return &SchemaDecoder{coercers: coercers}, nil
}

// ============================================================================
// Encoding (DecodedMessage -> RawSMsg)
// ============================================================================

// SchemaEncoder encodes DecodedMessages back to RawSMsg format using schemas.
// This enables round-trip encoding: RawSMsg -> DecodedMessage -> RawSMsg.
//
// The encoder validates that field values match their schema types and that
// all required (non-nullable) fields are present. Fields are encoded in the
// order they appear in the schema.
//
// Create a SchemaEncoder using NewSchemaEncoder.
type SchemaEncoder struct {
	schemas map[string]*Schema // map from record type name to schema
}

// Reverse coercion functions - convert Go types to byte strings

func encodeInt[T ~int | ~int8 | ~int16 | ~int32 | ~int64](v T) []byte {
	return []byte(strconv.FormatInt(int64(v), 10))
}

func encodeFloat32(v float32) []byte {
	return []byte(strconv.FormatFloat(float64(v), 'f', -1, 32))
}

func encodeFloat64(v float64) []byte {
	return []byte(strconv.FormatFloat(v, 'f', -1, 64))
}

var (
	_bFalse = []byte{'0'}
	_bTrue  = []byte{'1'}
)

func encodeBool(v bool) []byte {
	if v {
		return _bTrue
	}
	return _bFalse
}

func encodeString(v string) []byte {
	return []byte(v)
}

func encodeBytes(v []byte) []byte {
	return v
}

// encodeValue converts a typed value to bytes according to the field schema
func (e *SchemaEncoder) encodeValue(field *Field, value any) ([]byte, error) {
	// Handle nil for nullable fields
	if value == nil {
		if field.Nullable {
			return nil, nil // Signal to skip this field
		}
		return nil, fmt.Errorf("field %s is not nullable but has nil value", field.Name)
	}

	switch field.Type {
	case Int8Type:
		v, ok := value.(int8)
		if !ok {
			return nil, fmt.Errorf("field %s: expected int8, got %T", field.Name, value)
		}
		return encodeInt(v), nil

	case Int16Type:
		v, ok := value.(int16)
		if !ok {
			return nil, fmt.Errorf("field %s: expected int16, got %T", field.Name, value)
		}
		return encodeInt(v), nil

	case Int32Type:
		v, ok := value.(int32)
		if !ok {
			return nil, fmt.Errorf("field %s: expected int32, got %T", field.Name, value)
		}
		return encodeInt(v), nil

	case Int64Type:
		v, ok := value.(int64)
		if !ok {
			return nil, fmt.Errorf("field %s: expected int64, got %T", field.Name, value)
		}
		return encodeInt(v), nil

	case FloatType:
		v, ok := value.(float32)
		if !ok {
			return nil, fmt.Errorf("field %s: expected float32, got %T", field.Name, value)
		}
		return encodeFloat32(v), nil
	case DoubleType:
		v, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("field %s: expected float64, got %T", field.Name, value)
		}
		return encodeFloat64(v), nil

	case BoolType:
		v, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("field %s: expected bool, got %T", field.Name, value)
		}
		return encodeBool(v), nil

	case StringType:
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("field %s: expected string, got %T", field.Name, value)
		}
		return encodeString(v), nil

	case BinaryType:
		v, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("field %s: expected []byte, got %T", field.Name, value)
		}
		return encodeBytes(v), nil

	case EnumType:
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("field %s: expected string (enum), got %T", field.Name, value)
		}
		// Validate enum value
		enumValues := field.Metadata["enum_values"].([]any)
		valid := false
		for _, ev := range enumValues {
			if ev.(string) == v {
				valid = true
				break
			}
		}
		if !valid {
			return nil, fmt.Errorf("field %s: invalid enum value %q", field.Name, v)
		}
		return encodeString(v), nil

	default:
		return nil, fmt.Errorf("field %s: encoding type %s not yet implemented", field.Name, field.Type)
	}
}

// Encode converts a DecodedMessage back to RawSMsg format using the schema.
//
// The encoding process:
//  1. Looks up schema by RecordType name from the message
//  2. Verifies RecordTag matches the schema's tag
//  3. Encodes each field from the schema in order
//  4. Skips nullable fields that are nil or missing
//  5. Wraps fields in a constructor tag with the record type tag
//  6. Terminates the message properly
//
// Type conversions (reverse of decoding):
//   - int64 -> decimal string
//   - float64 -> decimal string (floating point notation)
//   - bool -> "1" (true) or "0" (false)
//   - string -> UTF-8 bytes
//   - []byte -> as-is
//   - enum values -> validated string
//
// Validation:
//   - All field values must match their expected types
//   - Required (non-nullable) fields must be present and non-nil
//   - Enum values must be in the allowed enum_values list
//   - RecordTag must match the schema's record type tag
//
// Returns an error if validation fails or if no schema exists for the record type.
// No partial messages are returned on error.
func (e *SchemaEncoder) Encode(msg *DecodedMessage) (*RawSMsg, error) {
	// 1. Lookup schema by record type name
	schema, ok := e.schemas[msg.RecordType]
	if !ok {
		return nil, fmt.Errorf("no schema found for record type %q", msg.RecordType)
	}

	// 2. Verify record tag matches schema
	recordTag, err := extractSmsgTag(schema.RecordType)
	if err != nil {
		return nil, fmt.Errorf("schema error: %w", err)
	}
	if recordTag != msg.RecordTag {
		return nil, fmt.Errorf("record tag mismatch: message has 0x%04X but schema expects 0x%04X",
			msg.RecordTag, recordTag)
	}

	// 3. Create inner message for fields
	var inner RawSMsg

	// 4. Encode each field from schema (in schema order)
	for i := range schema.Fields {
		field := &schema.Fields[i]
		value, exists := msg.Fields[field.Name]

		// Handle missing or nil fields
		if !exists || value == nil {
			if !field.Nullable {
				return nil, fmt.Errorf("required field %q is missing or nil", field.Name)
			}
			// Skip nullable fields that are missing or nil
			continue
		}

		// Convert value to bytes
		data, err := e.encodeValue(field, value)
		if err != nil {
			return nil, err
		}

		// Skip if encodeValue returned nil (shouldn't happen but be safe)
		if data == nil {
			continue
		}

		// Get tag from schema
		tag, err := extractSmsgTag(field)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", field.Name, err)
		}

		// Add to message
		inner.Add(tag, data)
	}

	// 5. Wrap in record type constructor (variable length)
	var outer RawSMsg
	outer.AddVariableTag(msg.RecordTag)
	outer.Data = append(outer.Data, inner.Data...)
	outer.Terminate()

	return &outer, nil
}

// NewSchemaEncoder creates a SchemaEncoder that can encode DecodedMessages
// back to RawSMsg format according to the provided schemas.
//
// Multiple schemas can be registered to handle different message types.
// The encoder selects the appropriate schema based on the DecodedMessage's
// RecordType field.
//
// Schema requirements:
//   - Each schema's RecordType must have an "smsg_tag" in its metadata
//   - All fields must have an "smsg_tag" in their metadata
//   - Schema record type names must be unique
//
// Returns an error if any schema is invalid or missing required metadata.
func NewSchemaEncoder(schemas []Schema) (*SchemaEncoder, error) {
	schemaMap := make(map[string]*Schema, len(schemas))
	for i := range schemas {
		schema := &schemas[i]
		if schema.RecordType == nil {
			return nil, fmt.Errorf("schema %d has nil RecordType", i)
		}
		// Verify schema has smsg_tag
		if _, err := extractSmsgTag(schema.RecordType); err != nil {
			return nil, fmt.Errorf("schema %s: %w", schema.RecordType.Name, err)
		}
		schemaMap[schema.RecordType.Name] = schema
	}

	return &SchemaEncoder{schemas: schemaMap}, nil
}
