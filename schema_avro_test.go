package gosmsg

import (
	"encoding/json"
	"testing"

	"github.com/hamba/avro/v2"
)

// Helper function to validate Avro schema using hamba/avro
func validateAvroSchema(t *testing.T, avroSchema map[string]interface{}) avro.Schema {
	t.Helper()

	// Marshal to JSON
	jsonBytes, err := json.Marshal(avroSchema)
	if err != nil {
		t.Fatalf("Failed to marshal Avro schema to JSON: %v", err)
	}

	// Parse with hamba/avro
	schema, err := avro.Parse(string(jsonBytes))
	if err != nil {
		t.Fatalf("Failed to parse Avro schema with hamba/avro: %v\nSchema JSON:\n%s", err, string(jsonBytes))
	}

	return schema
}

func TestSimpleTypesConversion(t *testing.T) {
	tests := []struct {
		name          string
		fieldType     DataType
		nullable      bool
		expectedAvro  string
		expectedAvro2 interface{} // For complex types
	}{
		{"bool", BoolType, false, "boolean", nil},
		{"int8", Int8Type, false, "int", nil},
		{"int16", Int16Type, false, "int", nil},
		{"int32", Int32Type, false, "int", nil},
		{"int64", Int64Type, false, "long", nil},
		{"string", StringType, false, "string", nil},
		{"float", FloatType, false, "float", nil},
		{"double", DoubleType, false, "double", nil},
		{"binary", BinaryType, false, "bytes", nil},
		{"nullable_string", StringType, true, "", []interface{}{"null", "string"}},
		{"nullable_int", Int32Type, true, "", []interface{}{"null", "int"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field, err := NewField(tt.name, tt.fieldType, tt.nullable, nil)
			if err != nil {
				t.Fatalf("Failed to create field: %v", err)
			}

			avroField, err := FieldToAvro(field, false)
			if err != nil {
				t.Fatalf("Failed to convert field to Avro: %v", err)
			}

			if tt.nullable {
				// Check union type
				typeVal, ok := avroField["type"].([]interface{})
				if !ok {
					t.Fatalf("Expected type to be []interface{} for nullable field, got %T", avroField["type"])
				}
				if len(typeVal) != 2 || typeVal[0] != "null" {
					t.Errorf("Expected nullable union [null, type], got %v", typeVal)
				}
				if avroField["default"] != nil {
					t.Errorf("Expected default to be nil for nullable field, got %v", avroField["default"])
				}
			} else {
				// Check simple type
				typeVal, ok := avroField["type"].(string)
				if !ok {
					t.Fatalf("Expected type to be string, got %T", avroField["type"])
				}
				if typeVal != tt.expectedAvro {
					t.Errorf("Expected type %s, got %s", tt.expectedAvro, typeVal)
				}
			}
		})
	}
}

func TestTimestampLogicalTypes(t *testing.T) {
	tests := []struct {
		name        string
		fieldType   DataType
		logicalType string
		baseType    string
	}{
		{"timestamp_ms", TimestampMsType, "timestamp-millis", "long"},
		{"timestamp_us", TimestampUsType, "timestamp-micros", "long"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field, err := NewField(tt.name, tt.fieldType, false, nil)
			if err != nil {
				t.Fatalf("Failed to create field: %v", err)
			}

			avroField, err := FieldToAvro(field, false)
			if err != nil {
				t.Fatalf("Failed to convert field to Avro: %v", err)
			}

			typeMap, ok := avroField["type"].(map[string]interface{})
			if !ok {
				t.Fatalf("Expected type to be map[string]interface{}, got %T", avroField["type"])
			}

			if typeMap["logicalType"] != tt.logicalType {
				t.Errorf("Expected logicalType %s, got %v", tt.logicalType, typeMap["logicalType"])
			}

			if typeMap["type"] != tt.baseType {
				t.Errorf("Expected base type %s, got %v", tt.baseType, typeMap["type"])
			}
		})
	}
}

func TestEnumConversion(t *testing.T) {
	enumValues := []interface{}{"RED", "GREEN", "BLUE"}
	metadata := map[string]interface{}{
		"enum_values": enumValues,
		"smsg_tag":    0x100,
	}

	field, err := NewField("color", EnumType, false, metadata)
	if err != nil {
		t.Fatalf("Failed to create enum field: %v", err)
	}

	avroField, err := FieldToAvro(field, false)
	if err != nil {
		t.Fatalf("Failed to convert enum field to Avro: %v", err)
	}

	typeMap, ok := avroField["type"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected type to be map[string]interface{}, got %T", avroField["type"])
	}

	if typeMap["type"] != "enum" {
		t.Errorf("Expected type 'enum', got %v", typeMap["type"])
	}

	if typeMap["name"] != "color" {
		t.Errorf("Expected name 'color', got %v", typeMap["name"])
	}

	symbols, ok := typeMap["symbols"].([]string)
	if !ok {
		t.Fatalf("Expected symbols to be []string, got %T", typeMap["symbols"])
	}

	if len(symbols) != 3 || symbols[0] != "RED" || symbols[1] != "GREEN" || symbols[2] != "BLUE" {
		t.Errorf("Expected symbols [RED, GREEN, BLUE], got %v", symbols)
	}
}

func TestArrayConversion(t *testing.T) {
	metadata := map[string]interface{}{
		"value_type": "string",
		"smsg_tag":   0x200,
	}

	field, err := NewField("tags", ArrayType, false, metadata)
	if err != nil {
		t.Fatalf("Failed to create array field: %v", err)
	}

	avroField, err := FieldToAvro(field, false)
	if err != nil {
		t.Fatalf("Failed to convert array field to Avro: %v", err)
	}

	typeMap, ok := avroField["type"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected type to be map[string]interface{}, got %T", avroField["type"])
	}

	if typeMap["type"] != "array" {
		t.Errorf("Expected type 'array', got %v", typeMap["type"])
	}

	// When value_type is a simple string, it defaults to nullable
	// So we expect a union: ["null", "string"]
	items, ok := typeMap["items"].([]interface{})
	if !ok {
		t.Fatalf("Expected items to be []interface{} for nullable value type, got %T", typeMap["items"])
	}
	if len(items) != 2 || items[0] != "null" || items[1] != "string" {
		t.Errorf("Expected items ['null', 'string'], got %v", items)
	}

	if _, ok := typeMap["default"]; !ok {
		t.Error("Expected default field for array")
	}
}

func TestMapConversion(t *testing.T) {
	metadata := map[string]interface{}{
		"value_type": "int64",
		"smsg_tag":   0x300,
	}

	field, err := NewField("counts", MapType, false, metadata)
	if err != nil {
		t.Fatalf("Failed to create map field: %v", err)
	}

	avroField, err := FieldToAvro(field, false)
	if err != nil {
		t.Fatalf("Failed to convert map field to Avro: %v", err)
	}

	typeMap, ok := avroField["type"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected type to be map[string]interface{}, got %T", avroField["type"])
	}

	if typeMap["type"] != "map" {
		t.Errorf("Expected type 'map', got %v", typeMap["type"])
	}

	// When value_type is a simple string, it defaults to nullable
	// So we expect a union: ["null", "long"]
	values, ok := typeMap["values"].([]interface{})
	if !ok {
		t.Fatalf("Expected values to be []interface{} for nullable value type, got %T", typeMap["values"])
	}
	if len(values) != 2 || values[0] != "null" || values[1] != "long" {
		t.Errorf("Expected values ['null', 'long'], got %v", values)
	}

	if _, ok := typeMap["default"]; !ok {
		t.Error("Expected default field for map")
	}
}

func TestNestedRecordConversion(t *testing.T) {
	metadata := map[string]interface{}{
		"smsg_tag": 0x400,
		"fields": []interface{}{
			map[string]interface{}{
				"name":     "lat",
				"type":     "double",
				"nullable": false,
			},
			map[string]interface{}{
				"name":     "lon",
				"type":     "double",
				"nullable": false,
			},
		},
	}

	field, err := NewField("location", RecordType, false, metadata)
	if err != nil {
		t.Fatalf("Failed to create record field: %v", err)
	}

	avroField, err := FieldToAvro(field, false)
	if err != nil {
		t.Fatalf("Failed to convert record field to Avro: %v", err)
	}

	typeMap, ok := avroField["type"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected type to be map[string]interface{}, got %T", avroField["type"])
	}

	if typeMap["type"] != "record" {
		t.Errorf("Expected type 'record', got %v", typeMap["type"])
	}

	if typeMap["name"] != "location" {
		t.Errorf("Expected name 'location', got %v", typeMap["name"])
	}

	fields, ok := typeMap["fields"].([]map[string]interface{})
	if !ok {
		t.Fatalf("Expected fields to be []map[string]interface{}, got %T", typeMap["fields"])
	}

	if len(fields) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(fields))
	}

	if fields[0]["name"] != "lat" || fields[0]["type"] != "double" {
		t.Errorf("Expected lat:double, got %v:%v", fields[0]["name"], fields[0]["type"])
	}

	if fields[1]["name"] != "lon" || fields[1]["type"] != "double" {
		t.Errorf("Expected lon:double, got %v:%v", fields[1]["name"], fields[1]["type"])
	}
}

func TestMetadataConversion(t *testing.T) {
	metadata := map[string]interface{}{
		"description": "User ID field",
		"smsg_tag":    0x500,
		"custom_prop": "custom_value",
	}

	field, err := NewField("user_id", Int64Type, false, metadata)
	if err != nil {
		t.Fatalf("Failed to create field: %v", err)
	}

	avroField, err := FieldToAvro(field, true)
	if err != nil {
		t.Fatalf("Failed to convert field to Avro: %v", err)
	}

	// Check doc field
	if avroField["doc"] != "User ID field" {
		t.Errorf("Expected doc 'User ID field', got %v", avroField["doc"])
	}

	// Check UTEL:metadata (should exclude description)
	utelMeta, ok := avroField["UTEL:metadata"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected UTEL:metadata to be map[string]interface{}, got %T", avroField["UTEL:metadata"])
	}

	if _, hasDesc := utelMeta["description"]; hasDesc {
		t.Error("UTEL:metadata should not contain description (it's in doc)")
	}

	if utelMeta["smsg_tag"] != 0x500 {
		t.Errorf("Expected smsg_tag 0x500, got %v", utelMeta["smsg_tag"])
	}

	if utelMeta["custom_prop"] != "custom_value" {
		t.Errorf("Expected custom_prop 'custom_value', got %v", utelMeta["custom_prop"])
	}
}

func TestSchemaToAvro(t *testing.T) {
	// Create a schema with multiple field types
	recordMetadata := map[string]interface{}{
		"description": "Test record schema",
		"smsg_tag":    0x1000,
	}

	recordType, err := NewField("test_record", RecordType, false, recordMetadata)
	if err != nil {
		t.Fatalf("Failed to create record type: %v", err)
	}

	fields := []Field{}

	// Add various field types
	field1, _ := NewField("id", Int64Type, false, map[string]interface{}{"smsg_tag": 0x1001})
	fields = append(fields, *field1)

	field2, _ := NewField("name", StringType, true, map[string]interface{}{"smsg_tag": 0x1002})
	fields = append(fields, *field2)

	field3, _ := NewField("score", DoubleType, false, map[string]interface{}{"smsg_tag": 0x1003})
	fields = append(fields, *field3)

	schema, err := NewSchema(recordType, fields, 1)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	// Convert to Avro
	avroSchema, err := SchemaToAvro(schema, "com.example", true)
	if err != nil {
		t.Fatalf("Failed to convert schema to Avro: %v", err)
	}

	// Validate basic structure
	if avroSchema["name"] != "test_record" {
		t.Errorf("Expected name 'test_record', got %v", avroSchema["name"])
	}

	if avroSchema["type"] != "record" {
		t.Errorf("Expected type 'record', got %v", avroSchema["type"])
	}

	if avroSchema["namespace"] != "com.example" {
		t.Errorf("Expected namespace 'com.example', got %v", avroSchema["namespace"])
	}

	if avroSchema["doc"] != "Test record schema" {
		t.Errorf("Expected doc 'Test record schema', got %v", avroSchema["doc"])
	}

	// Validate with hamba/avro
	validateAvroSchema(t, avroSchema)
}

func TestSchemaToAvroJSON(t *testing.T) {
	recordType, err := NewField("simple_record", RecordType, false, map[string]interface{}{
		"smsg_tag": 0x2000,
	})
	if err != nil {
		t.Fatalf("Failed to create record type: %v", err)
	}

	field1, _ := NewField("message", StringType, false, map[string]interface{}{"smsg_tag": 0x2001})
	fields := []Field{*field1}

	schema, err := NewSchema(recordType, fields, 1)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	jsonStr, err := SchemaToAvroJSON(schema, "", false)
	if err != nil {
		t.Fatalf("Failed to convert schema to JSON: %v", err)
	}

	// Parse back to verify it's valid JSON
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &parsed); err != nil {
		t.Fatalf("Failed to parse generated JSON: %v", err)
	}

	// Validate with hamba/avro
	_, err = avro.Parse(jsonStr)
	if err != nil {
		t.Fatalf("Failed to parse generated JSON with hamba/avro: %v\nJSON:\n%s", err, jsonStr)
	}
}

func TestComplexSchemaValidation(t *testing.T) {
	// Create a complex schema similar to the SIP example in architecture.md
	recordMetadata := map[string]interface{}{
		"description": "SIP call record",
		"smsg_tag":    0x1019,
	}

	recordType, err := NewField("sip", RecordType, false, recordMetadata)
	if err != nil {
		t.Fatalf("Failed to create record type: %v", err)
	}

	fields := []Field{}

	// start_ts: int64, not nullable
	f1, _ := NewField("start_ts", Int64Type, false, map[string]interface{}{"smsg_tag": 0x1020})
	fields = append(fields, *f1)

	// duration: int32, nullable
	f2, _ := NewField("duration", Int32Type, true, map[string]interface{}{"smsg_tag": 0x1021})
	fields = append(fields, *f2)

	// caller: string, not nullable
	f3, _ := NewField("caller", StringType, false, map[string]interface{}{"smsg_tag": 0x1030})
	fields = append(fields, *f3)

	// app: enum, not nullable
	f4, _ := NewField("app", EnumType, false, map[string]interface{}{
		"smsg_tag":    0x1040,
		"enum_values": []interface{}{"CAP", "MAP", "INAP"},
	})
	fields = append(fields, *f4)

	// headers: map of string, nullable
	f5, _ := NewField("headers", MapType, true, map[string]interface{}{
		"smsg_tag":   0x1050,
		"value_type": "string",
	})
	fields = append(fields, *f5)

	// call_legs: array of records, not nullable
	f6, _ := NewField("call_legs", ArrayType, false, map[string]interface{}{
		"smsg_tag": 0x1060,
		"value_type": map[string]interface{}{
			"type":     "record",
			"nullable": false,
			"metadata": map[string]interface{}{
				"fields": []interface{}{
					map[string]interface{}{
						"name":     "direction",
						"type":     "string",
						"nullable": false,
					},
					map[string]interface{}{
						"name":     "number",
						"type":     "string",
						"nullable": false,
					},
				},
			},
		},
	})
	fields = append(fields, *f6)

	schema, err := NewSchema(recordType, fields, 1)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	// Convert to Avro
	avroSchema, err := SchemaToAvro(schema, "", true)
	if err != nil {
		t.Fatalf("Failed to convert schema to Avro: %v", err)
	}

	// Validate with hamba/avro - this is the key test!
	avroSchemaObj := validateAvroSchema(t, avroSchema)

	// Additional validation - check the schema can be used
	if avroSchemaObj == nil {
		t.Fatal("Parsed Avro schema is nil")
	}

	// Print the schema for debugging (optional)
	jsonBytes, _ := json.MarshalIndent(avroSchema, "", "  ")
	t.Logf("Generated Avro schema:\n%s", string(jsonBytes))
}

func TestErrorCases(t *testing.T) {
	t.Run("nil_schema", func(t *testing.T) {
		_, err := SchemaToAvro(nil, "", false)
		if err == nil {
			t.Error("Expected error for nil schema")
		}
	})

	t.Run("enum_without_values", func(t *testing.T) {
		field, _ := NewField("bad_enum", EnumType, false, map[string]interface{}{
			"smsg_tag": 0x100,
		})
		_, err := FieldToAvro(field, false)
		if err == nil {
			t.Error("Expected error for enum without enum_values")
		}
	})

	t.Run("array_without_value_type", func(t *testing.T) {
		// Create field bypassing NewField validation
		field := &Field{
			Name:      "bad_array",
			Type:      ArrayType,
			Nullable:  false,
			Metadata:  map[string]interface{}{"smsg_tag": 0x200},
			ValueType: nil,
		}
		_, err := FieldToAvro(field, false)
		if err == nil {
			t.Error("Expected error for array without value_type")
		}
	})

	t.Run("map_without_value_type", func(t *testing.T) {
		// Create field bypassing NewField validation
		field := &Field{
			Name:      "bad_map",
			Type:      MapType,
			Nullable:  false,
			Metadata:  map[string]interface{}{"smsg_tag": 0x300},
			ValueType: nil,
		}
		_, err := FieldToAvro(field, false)
		if err == nil {
			t.Error("Expected error for map without value_type")
		}
	})

	t.Run("record_without_fields", func(t *testing.T) {
		// Create field bypassing NewField validation
		field := &Field{
			Name:     "bad_record",
			Type:     RecordType,
			Nullable: false,
			Metadata: map[string]interface{}{"smsg_tag": 0x400},
			Fields:   []Field{},
		}
		_, err := FieldToAvro(field, false)
		if err == nil {
			t.Error("Expected error for record without fields")
		}
	})
}
