package gosmsg

import (
	"fmt"
	"strconv"
	"strings"
)

// Default capacity for pre-allocated maps and slices
const defaultCapacity = 64

// Fields represents the decoded field values in an SMSG message,
// mapping field names to their typed values
type Fields map[string]interface{}

// DecodedMessage represents a decoded SMSG message with its record type
// metadata and field values
type DecodedMessage struct {
	RecordType string // Name of the record type (e.g., "sip", "call_detail")
	RecordTag  uint16 // Numeric tag identifying the record type (e.g., 0x1019)
	Fields     Fields // Decoded field values keyed by field name
}

// String returns a string representation of the decoded message for debugging
func (d *DecodedMessage) String() string {
	return fmt.Sprintf("DecodedMessage{RecordType: %s, RecordTag: 0x%04X, Fields: %d}",
		d.RecordType, d.RecordTag, len(d.Fields))
}

// Function to coerce a SMSG field to schema determined type
type coerceFunc func(field *fieldData, val []byte) (interface{}, error)

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

func coerceToString(_ *fieldData, val []byte) (interface{}, error) {
	return strings.ToValidUTF8(string(val), "?"), nil
}

func coerceToInt(_ *fieldData, val []byte) (interface{}, error) {
	return strconv.ParseInt(string(val), 10, 64)
}

func coerceToFloat64(_ *fieldData, val []byte) (interface{}, error) {
	return strconv.ParseFloat(string(val), 64)
}

func coerceToBool(_ *fieldData, val []byte) (interface{}, error) {
	return val[0] != '0', nil
}

func coerceToEnum(f *fieldData, val []byte) (interface{}, error) {
	s := string(val)
	if _, ok := f.enumValues[s]; !ok {
		return "", fmt.Errorf("invalid enum value %s for %s", s, f.name)
	}
	return s, nil // Guaranteed valid string at this point
}
func coerceToBytes(_ *fieldData, val []byte) (interface{}, error) {
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
		// validateEnumMetadata ensures enum_values is []interface{} containing only strings
		enumValuesRaw := f.Metadata["enum_values"].([]interface{})
		for _, v := range enumValuesRaw {
			enumMap[v.(string)] = true
		}
		coerceFunc = coerceToEnum
	case Int8Type, Int16Type, Int32Type, Int64Type:
		coerceFunc = coerceToInt
	case FloatType, DoubleType:
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
	//
	// Fill out all field names from the schema, convert raw tag value to the field data type,

	sc, ok := s.coercers[recordType.Tag]
	if !ok {
		return nil, &MissingSchemaError{Tag: recordType.Tag}
	}

	fields := make(Fields, len(sc.fields))
	for i := range sc.fields {
		fd := &sc.fields[i]

		rawVal, ok := tags[fd.smsgTag]
		if !ok {
			if fd.isNullable {
				fields[fd.name] = nil
			} else {
				return &DecodedMessage{
					RecordType: sc.recordTypeName,
					RecordTag:  recordType.Tag,
					Fields:     fields,
				}, fmt.Errorf("Field %s is missing from record, but not nullable", fd.name)
			}
		} else {
			val, err := fd.coerceFunc(fd, rawVal)
			if err != nil {
				return &DecodedMessage{
					RecordType: sc.recordTypeName,
					RecordTag:  recordType.Tag,
					Fields:     fields,
				}, fmt.Errorf("failed converting %s in %s:%s : %w", rawVal, sc.recordTypeName, fd.name, err)
			}
			fields[fd.name] = val
		}
	}

	return &DecodedMessage{
		RecordType: sc.recordTypeName,
		RecordTag:  recordType.Tag,
		Fields:     fields,
	}, nil
}

// Decode uses the registred schemas to decode the RawSMsg and returns
// the decoded message with record type information and field values.
//
// Any errors when parsing or converting the message is returned.
// A partially decoded message might be returned even if the error is non-nil.
// If no schemas match the message, an instance of the MissingSchemaError is returned.
// Use this to check for MissingSchemaError:
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

// NewSchemaDecoder returns a SchemaDecoder which can decode
// SMSGs according to the given schemas.
//
// Decoding an SMSG will convert numeric tags to field names, convert the value to
// a proper data type and fill in missing nullable fields.
//
// Returns error if schemas doesn't contain proper info to decode an SMSG
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

// SchemaEncoder encodes DecodedMessages back to RawSMsg format using schemas
type SchemaEncoder struct {
	schemas map[string]*Schema // map from record type name to schema
}

// Reverse coercion functions - convert Go types to byte strings

func encodeInt64(v int64) []byte {
	return []byte(strconv.FormatInt(v, 10))
}

func encodeFloat64(v float64) []byte {
	return []byte(strconv.FormatFloat(v, 'f', -1, 64))
}

func encodeBool(v bool) []byte {
	if v {
		return []byte("1")
	}
	return []byte("0")
}

func encodeString(v string) []byte {
	return []byte(v)
}

func encodeBytes(v []byte) []byte {
	return v
}

// encodeValue converts a typed value to bytes according to the field schema
func (e *SchemaEncoder) encodeValue(field *Field, value interface{}) ([]byte, error) {
	// Handle nil for nullable fields
	if value == nil {
		if field.Nullable {
			return nil, nil // Signal to skip this field
		}
		return nil, fmt.Errorf("field %s is not nullable but has nil value", field.Name)
	}

	switch field.Type {
	case Int8Type, Int16Type, Int32Type, Int64Type:
		v, ok := value.(int64)
		if !ok {
			return nil, fmt.Errorf("field %s: expected int64, got %T", field.Name, value)
		}
		return encodeInt64(v), nil

	case FloatType, DoubleType:
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
		enumValues := field.Metadata["enum_values"].([]interface{})
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
// Returns error if the message doesn't match the schema or contains invalid values.
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

// NewSchemaEncoder returns a SchemaEncoder which can encode
// DecodedMessages back to RawSMsg format according to the given schemas.
//
// Encoding converts typed field values to byte strings and wraps them
// with the appropriate tags based on the schema.
//
// Returns error if schemas are invalid.
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
