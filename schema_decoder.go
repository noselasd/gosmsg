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
