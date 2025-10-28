package gosmsg

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Default capacity for pre-allocated maps and slices
const defaultCapacity = 64

// Function to coerce a SMSG field to schema determined type
type coerceFunc func(field *fieldData, val []byte) (interface{}, error)

// pre-computed conversion help for a field
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

func extractSmsgTag(field *Field) (uint16, error) {
	smsgTag, ok := field.Metadata["smsg_tag"]
	if !ok {
		return 0, &SchemaError{Message: fmt.Sprintf("%s is missing smsg_tag metadata", field.Name)}
	}
	smsgTagInt, ok := smsgTag.(uint16)
	if !ok {
		return 0, &SchemaError{Message: fmt.Sprintf("%s smsg_tag metadata must be an int", field.Name)}
	}
	return smsgTagInt, nil
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
		return "", &SchemaValidationError{Message: fmt.Sprintf("Invalid enum value %s for %s", s, f.name)}
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
		enumValues := f.Metadata["enum_values"].([]string) // schema loading validated this
		for _, v := range enumValues {
			enumMap[v] = true
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
		return fieldData{}, &SchemaError{Message: fmt.Sprintf("Type conversion of %s is not implemented", f.Name)}
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

func (s *SchemaDecoder) coerce(recordType *Tag, tags map[uint16][]byte) (map[string]interface{}, error) {
	//
	// Fill out all field names from the schema, convert raw tag value to the field data type,

	sc, ok := s.coercers[recordType.Tag]
	if !ok {
		return nil, &RecordTypeMismatchError{Message: fmt.Sprintf("Record tag 0x%04X does not match any schemas", recordType.Tag)}
	}
	dc := make(map[string]interface{}, len(sc.fields))
	for i := range sc.fields {
		fd := &sc.fields[i]

		t, ok := tags[fd.smsgTag]
		if !ok {
			if fd.isNullable {
				dc[fd.name] = nil
			} else {
				return dc, &SchemaValidationError{Message: fmt.Sprintf("Field %s is missing from record, but not nullable", fd.name)}
			}
		} else {
			val, err := fd.coerceFunc(fd, t)
			if err != nil {
				return dc, err
			}
			dc[fd.name] = val
		}
	}
	return dc, nil
}

func (s *SchemaDecoder) Decode(r *RawSMsg) (map[string]interface{}, error) {
	it := r.Tags()

	recordType, err := it.NextTag()
	if err != nil {
		return nil, err
	}
	tags := make(map[uint16][]byte, defaultCapacity)
	for t, err := it.NextTag(); err != io.EOF; t, err = it.NextTag() {
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
