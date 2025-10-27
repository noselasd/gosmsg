package gosmsg

import (
	"fmt"
	"strconv"
	"strings"
)

// Function to coerce a SMSG field to schema determined type
type coerceFunc func(field *fieldData, val []byte) (interface{}, error)

type fieldData struct {
	nullable   bool
	isString   bool
	smsgTag    int
	name       string
	enumValues map[string]bool
	coerceFunc coerceFunc
}

type schemaCoercion struct {
	recordTypeName string
	recordTypeTag  int
	fields         []fieldData
}

type SchemaDecoder struct {
	coercers map[int]schemaCoercion // map from record type tag to schemaCoersion
}

func extractSmsgTag(field *Field) (int, error) {
	smsg_tag, ok := field.Metadata["smsg_tag"]
	if !ok {
		return 0, &SchemaError{Message: fmt.Sprintf("%s is missing smsg_tag metadata", field.Name)}
	}
	smsg_tag_int, ok := smsg_tag.(int)
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

func coerceToFloat(_ *fieldData, val []byte) (interface{}, error) {
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
	// We convert all integers to int64, like pysmsg. This may be a mistake.
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
		coerceFunc = coerceToFloat
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
		nullable:   f.Nullable,
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

func NewSchemaDecoder(schemas []Schema) (*SchemaDecoder, error) {
	coercers := make(map[int]schemaCoercion, 64)
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
