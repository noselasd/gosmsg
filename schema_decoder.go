package gosmsg

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

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
	smsg_tag, ok := field.Metadata["smsg_tag"]
	if !ok {
		return 0, &SchemaError{Message: fmt.Sprintf("%s is missing smsg_tag metadata", field.Name)}
	}
	smsg_tag_int, ok := smsg_tag.(uint16)
	if !ok {
		return 0, &SchemaError{Message: fmt.Sprintf("%s smsg_tag metadata must be an int", field.Name)}
	}
	return smsg_tag_int, nil
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
	if _, ok := f.enumValues[s]; ok {
		return "", &SchemaValidationError{Message: fmt.Sprintf("Invalid enum value %s for %s", s, f.name)}
	}
	return s, nil // Guaranteed valid string at this point
}
func coerceToBytes(_ *fieldData, val []byte) (interface{}, error) {
	return val, nil
}
func newFieldData(f *Field) (*fieldData, error) {
	smsg_tag, err := extractSmsgTag(f)
	if err != nil {
		return nil, err
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
		return nil, &SchemaError{Message: fmt.Sprintf("Type conversion of %s is not implemented", f.Name)}
	}

	return &fieldData{
		isNullable: f.Nullable,
		isString:   f.Type == StringType,
		smsgTag:    smsg_tag,
		name:       f.Name,
		enumValues: enumMap,
		coerceFunc: coerceFunc,
	}, nil
}

func newSchemaCoercion(s *Schema) (*schemaCoercion, error) {
	smsg_tag, err := extractSmsgTag(s.RecordType)
	if err != nil {
		return nil, err
	}

	fields := make([]fieldData, 0, 64)
	for i := range s.Fields {
		f := &s.Fields[i]
		d, err := newFieldData(f)
		if err != nil {
			return nil, err
		}
		fields[i] = *d
	}

	return &schemaCoercion{
		recordTypeName: s.RecordType.Name,
		recordTypeTag:  smsg_tag,
		fields:         fields,
	}, nil
}

func (s *SchemaDecoder) coerce(recordType *Tag, tags map[uint16][]byte) (map[string]interface{}, error) {
	dc := make(map[string]interface{}, 64)

	sc, ok := s.coercers[recordType.Tag]
	if !ok {
		return nil, &RecordTypeMismatchError{Message: fmt.Sprintf("Record tag 0x%04X does not match any schemas", recordType.Tag)}
	}
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
	tags := make(map[uint16][]byte, 64)
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
	coercers := make(map[uint16]schemaCoercion, 64)
	for i := range schemas {
		schema := &schemas[i]
		c, err := newSchemaCoercion(schema)
		if err != nil {
			return nil, err
		}
		coercers[c.recordTypeTag] = *c
	}

	return &SchemaDecoder{coercers: coercers}, nil
}
