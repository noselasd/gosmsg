// Package gosmsg provides conversion from gosmsg schemas to Avro schemas.
package gosmsg

import (
	"encoding/json"
	"fmt"
)

var gosmsgToAvroTypeMap = map[DataType]string{
	BoolType:        "boolean",
	Int8Type:        "int",
	Int16Type:       "int",
	Int32Type:       "int",
	Int64Type:       "long",
	StringType:      "string",
	FloatType:       "float",
	DoubleType:      "double",
	BinaryType:      "bytes",
	TimestampMsType: "long",
	TimestampUsType: "long",
	EnumType:        "enum",
	ArrayType:       "array",
	MapType:         "map",
	RecordType:      "record",
}

var gosmsgToAvroLogicalType = map[DataType]string{
	TimestampMsType: "timestamp-millis",
	TimestampUsType: "timestamp-micros",
}

// addMetadata adds UTEL:metadata to the avro element, excluding description
// which is already added as 'doc'
func addMetadata(smsgMetadata map[string]interface{}, avroElement map[string]interface{}) {
	if smsgMetadata == nil {
		return
	}

	// Remove description from metadata as it's already added to the avro element
	filtered := make(map[string]interface{})
	for k, v := range smsgMetadata {
		if k != "description" {
			filtered[k] = v
		}
	}

	if len(filtered) > 0 {
		avroElement["UTEL:metadata"] = filtered
	}
}

// addDoc adds the 'doc' field if description exists in metadata
func addDoc(field *Field, avroElement map[string]interface{}) {
	if doc, ok := field.Metadata["description"].(string); ok && doc != "" {
		avroElement["doc"] = doc
	}
}

// getAvroTypeForValueType gets the Avro type for array items or map values.
// This handles nullable value types properly by creating a union with null if needed.
func getAvroTypeForValueType(field *Field, addMetadataFlag bool) (interface{}, error) {
	if field == nil {
		return nil, &SchemaConversionError{Message: "value type field cannot be nil"}
	}

	avroType, ok := gosmsgToAvroTypeMap[field.Type]
	if !ok {
		return nil, &SchemaConversionError{
			Message: fmt.Sprintf("field %s with type %s cannot be converted to avro type", field.Name, field.Type),
		}
	}

	var typeValue interface{}

	if logicalType, hasLogical := gosmsgToAvroLogicalType[field.Type]; hasLogical {
		typeValue = map[string]interface{}{
			"logicalType": logicalType,
			"type":        avroType,
		}
	} else if field.Type == EnumType {
		enumValues, ok := field.Metadata["enum_values"].([]interface{})
		if !ok {
			return nil, &SchemaConversionError{
				Message: fmt.Sprintf("enum field %s must have enum_values in metadata", field.Name),
			}
		}

		symbols := make([]string, len(enumValues))
		for i, v := range enumValues {
			s, ok := v.(string)
			if !ok {
				return nil, &SchemaConversionError{
					Message: fmt.Sprintf("enum values must be strings for field %s", field.Name),
				}
			}
			symbols[i] = s
		}

		typeValue = map[string]interface{}{
			"type":    avroType,
			"name":    field.Name,
			"symbols": symbols,
		}
	} else if field.Type == RecordType {
		if len(field.Fields) == 0 {
			return nil, &SchemaConversionError{
				Message: fmt.Sprintf("record field %s must have fields", field.Name),
			}
		}

		avroFields := make([]map[string]interface{}, len(field.Fields))
		for i, f := range field.Fields {
			af, err := FieldToAvro(&f, addMetadataFlag)
			if err != nil {
				return nil, err
			}
			avroFields[i] = af
		}

		typeValue = map[string]interface{}{
			"name":   field.Name,
			"type":   avroType,
			"fields": avroFields,
		}
	} else {
		typeValue = avroType
	}

	if field.Nullable {
		return []interface{}{"null", typeValue}, nil
	}

	return typeValue, nil
}

// addAvroType adds the type information to the avro field
func addAvroType(field *Field, avroField map[string]interface{}, addMetadataFlag bool) error {
	avroType, ok := gosmsgToAvroTypeMap[field.Type]
	if !ok {
		return &SchemaConversionError{
			Message: fmt.Sprintf("field %s with type %s cannot be converted to avro type", field.Name, field.Type),
		}
	}

	var typeValue interface{}

	// Handle logical types (timestamps)
	if logicalType, hasLogical := gosmsgToAvroLogicalType[field.Type]; hasLogical {
		typeValue = map[string]interface{}{
			"logicalType": logicalType,
			"type":        avroType,
		}
	} else if field.Type == EnumType {
		// Enum type
		enumValues, ok := field.Metadata["enum_values"].([]interface{})
		if !ok {
			return &SchemaConversionError{
				Message: fmt.Sprintf("enum field %s must have enum_values in metadata", field.Name),
			}
		}

		symbols := make([]string, len(enumValues))
		for i, v := range enumValues {
			s, ok := v.(string)
			if !ok {
				return &SchemaConversionError{
					Message: fmt.Sprintf("enum values must be strings for field %s", field.Name),
				}
			}
			symbols[i] = s
		}

		typeValue = map[string]interface{}{
			"type":    avroType,
			"name":    field.Name,
			"symbols": symbols,
		}
	} else if field.Type == ArrayType {
		if field.ValueType == nil {
			return &SchemaConversionError{
				Message: fmt.Sprintf("array field %s must have value_type", field.Name),
			}
		}

		// Get the type for array items - we need to extract the base type
		// without the nullable wrapper since array items handle nullability differently
		itemType, err := getAvroTypeForValueType(field.ValueType, addMetadataFlag)
		if err != nil {
			return err
		}

		typeValue = map[string]interface{}{
			"type":    avroType,
			"items":   itemType,
			"default": []interface{}{},
		}
	} else if field.Type == MapType {
		if field.ValueType == nil {
			return &SchemaConversionError{
				Message: fmt.Sprintf("map field %s must have value_type", field.Name),
			}
		}

		// Get the type for map values - we need to extract the base type
		// without the nullable wrapper since map values handle nullability differently
		valueType, err := getAvroTypeForValueType(field.ValueType, addMetadataFlag)
		if err != nil {
			return err
		}

		typeValue = map[string]interface{}{
			"type":    avroType,
			"values":  valueType,
			"default": map[string]interface{}{},
		}
	} else if field.Type == RecordType {
		if len(field.Fields) == 0 {
			return &SchemaConversionError{
				Message: fmt.Sprintf("record field %s must have fields", field.Name),
			}
		}

		avroFields := make([]map[string]interface{}, len(field.Fields))
		for i, f := range field.Fields {
			af, err := FieldToAvro(&f, addMetadataFlag)
			if err != nil {
				return err
			}
			avroFields[i] = af
		}

		typeValue = map[string]interface{}{
			"name":   field.Name,
			"type":   avroType,
			"fields": avroFields,
		}
	} else {
		typeValue = avroType
	}

	if field.Nullable {
		avroField["type"] = []interface{}{"null", typeValue}
		avroField["default"] = nil
	} else {
		avroField["type"] = typeValue
	}

	return nil
}

// FieldToAvro converts a gosmsg Field to an Avro field representation.
//
// Args:
//   - field: Field to convert
//   - addMetadataFlag: If true, add UTEL:metadata node to the Avro field
//
// Returns:
//   - A map representing the Avro field with keys: "name", "doc" (optional),
//     "type", "logicalType" (optional), and "UTEL:metadata" (optional)
//   - An error if the field cannot be converted
func FieldToAvro(field *Field, addMetadataFlag bool) (map[string]interface{}, error) {
	if field == nil {
		return nil, &SchemaConversionError{Message: "field cannot be nil"}
	}

	avroField := map[string]interface{}{
		"name": field.Name,
	}

	if err := addAvroType(field, avroField, addMetadataFlag); err != nil {
		return nil, err
	}

	if addMetadataFlag {
		addMetadata(field.Metadata, avroField)
	}

	addDoc(field, avroField)

	return avroField, nil
}

// SchemaToAvro converts a gosmsg Schema to an Avro schema.
//
// See https://avro.apache.org/docs/1.11.1/specification/ for info on Avro schemas.
//
// Metadata conversion:
//   - 'description' is converted to the Avro schema 'doc' element
//   - Other metadata are added to the Avro schema in a 'UTEL:metadata' dict
//
// Args:
//   - schema: Schema to convert
//   - namespace: Namespace attribute of Avro record (optional, pass empty string for none)
//   - addMetadataFlag: If true, add UTEL:metadata node to fields
//
// Returns:
//   - An Avro schema as a map[string]interface{}
//   - An error if the schema cannot be converted
func SchemaToAvro(schema *Schema, namespace string, addMetadataFlag bool) (map[string]interface{}, error) {
	if schema == nil {
		return nil, &SchemaConversionError{Message: "schema cannot be nil"}
	}

	if schema.RecordType == nil {
		return nil, &SchemaConversionError{Message: "schema.RecordType cannot be nil"}
	}

	avroSchema := map[string]interface{}{
		"name": schema.RecordType.Name,
		"type": "record", // All smsg schemas are Avro record types
	}

	if namespace != "" {
		avroSchema["namespace"] = namespace
	}

	addDoc(schema.RecordType, avroSchema)

	if addMetadataFlag {
		addMetadata(schema.RecordType.Metadata, avroSchema)
	}

	avroFields := make([]map[string]interface{}, len(schema.Fields))
	for i, field := range schema.Fields {
		avroField, err := FieldToAvro(&field, addMetadataFlag)
		if err != nil {
			return nil, err
		}
		avroFields[i] = avroField
	}

	avroSchema["fields"] = avroFields

	return avroSchema, nil
}

// SchemaToAvroJSON converts a gosmsg Schema to an Avro schema JSON string.
//
// This is a convenience function that calls SchemaToAvro and marshals the result to JSON.
//
// Args:
//   - schema: Schema to convert
//   - namespace: Namespace attribute of Avro record (optional, pass empty string for none)
//   - addMetadataFlag: If true, add UTEL:metadata node to fields
//
// Returns:
//   - An Avro schema as a JSON string
//   - An error if the schema cannot be converted or marshaled
func SchemaToAvroJSON(schema *Schema, namespace string, addMetadataFlag bool) (string, error) {
	avroSchema, err := SchemaToAvro(schema, namespace, addMetadataFlag)
	if err != nil {
		return "", err
	}

	jsonBytes, err := json.MarshalIndent(avroSchema, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal avro schema to JSON: %w", err)
	}

	return string(jsonBytes), nil
}
