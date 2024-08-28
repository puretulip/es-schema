package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/apache/arrow/go/v10/parquet"
	"github.com/apache/arrow/go/v10/parquet/compress"
	"github.com/apache/arrow/go/v10/parquet/pqarrow"
)

func main() {
	// JSON 매핑 테이블
	mapping := `{
    "properties": {
        "user": {
            "properties": {
                "name": { "type": "text" },
                "address": {
                    "type": "nested",
                    "properties": {
                        "street": { "type": "text" },
                        "city": { "type": "text" },
                        "zipcode": { "type": "integer" }
                    }
                },
                "tags": { "type": "keyword" },
                "scores": { "type": "float" }
            },
            "type": "nested"
        },
        "timestamp": { "type": "date" }
    }
}`

	// JSON 파싱
	var esMapping map[string]interface{}
	err := json.Unmarshal([]byte(mapping), &esMapping)
	if err != nil {
		log.Fatalf("Error parsing JSON: %v", err)
	}

	// Arrow 스키마 생성
	fields := parseProperties(esMapping["properties"].(map[string]interface{}))
	originalSchema := arrow.NewSchema(fields, nil)

	// 원래 스키마 출력
	fmt.Println("Original Schema:")
	for _, field := range originalSchema.Fields() {
		fmt.Printf("  %s: %s\n", field.Name, field.Type)
	}

	// 고정된 샘플 데이터 생성
	sampleData := generateSampleData()

	// 스키마 조정 (리스트 타입 확인)
	adjustedSchema := adjustSchemaForLists(originalSchema, sampleData)

	// 변경된 스키마 출력
	fmt.Println("\nAdjusted Schema:")
	for _, field := range adjustedSchema.Fields() {
		fmt.Printf("  %s: %s\n", field.Name, field.Type)
	}

	// Arrow 레코드 생성
	record := createArrowRecord(adjustedSchema, sampleData)

	fmt.Println("\nArrow Record:", record)

	// Parquet 파일로 저장
	outputFile, err := os.Create("output.parquet")
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer outputFile.Close()

	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	arrowWriterProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())

	writer, err := pqarrow.NewFileWriter(
		record.Schema(),
		outputFile,
		writerProps,
		arrowWriterProps,
	)
	if err != nil {
		log.Fatalf("Failed to create Parquet writer: %v", err)
	}

	if err := writer.Write(record); err != nil {
		log.Fatalf("Failed to write record to Parquet file: %v", err)
	}

	if err := writer.Close(); err != nil {
		log.Fatalf("Failed to close Parquet writer: %v", err)
	}

	fmt.Println("Parquet file created successfully: output.parquet")
}

func generateSampleData() []map[string]interface{} {
	return []map[string]interface{}{
		{
			"user": map[string]interface{}{
				"name": "John Doe",
				"address": map[string]interface{}{
					"street":  "123 Main St",
					"city":    "New York",
					"zipcode": 10001,
				},
				"tags":   []string{"developer", "golang"},
				"scores": []float32{85.5, 92.0, 78.5},
			},
			"timestamp": time.Now(),
		},
		{
			"user": map[string]interface{}{
				"name": "Jane Smith",
				"address": map[string]interface{}{
					"street":  "456 Elm St",
					"city":    "Los Angeles",
					"zipcode": 90001,
				},
				"tags":   []string{"designer", "ui/ux"},
				"scores": []float32{88.0, 95.5},
			},
			"timestamp": time.Now().Add(-24 * time.Hour),
		},
		{
			"user": map[string]interface{}{
				"name": "Bob Johnson",
				"address": map[string]interface{}{
					"street":  "789 Oak St",
					"city":    "Chicago",
					"zipcode": 60601,
				},
				"tags":   "manager",
				"scores": []float32{79.0, 82.5, 91.0, 87.5},
			},
			"timestamp": time.Now().Add(-48 * time.Hour),
		},
	}
}

// parseProperties 함수는 주어진 properties 맵을 순회하여 Arrow 필드 목록을 생성합니다.
func parseProperties(properties map[string]interface{}) []arrow.Field {
	fields := []arrow.Field{}
	for fieldName, fieldProperties := range properties {
		fieldProps := fieldProperties.(map[string]interface{})
		fieldType, ok := fieldProps["type"].(string)
		if !ok {
			// "type"이 없는 경우 "object"로 가정
			fieldType = "object"
		}
		arrowType := esTypeToArrowType(fieldType, fieldProps)
		fields = append(fields, arrow.Field{Name: fieldName, Type: arrowType})
	}
	return fields
}

// esTypeToArrowType 함수는 Elasticsearch 타입을 Arrow 타입으로 매핑합니다.
func esTypeToArrowType(esType string, fieldProps map[string]interface{}) arrow.DataType {
	switch esType {
	case "text", "keyword":
		return arrow.BinaryTypes.String
	case "integer":
		return arrow.PrimitiveTypes.Int32
	case "long":
		return arrow.PrimitiveTypes.Int64
	case "float":
		return arrow.PrimitiveTypes.Float32
	case "double":
		return arrow.PrimitiveTypes.Float64
	case "boolean":
		return arrow.FixedWidthTypes.Boolean
	case "date":
		// Date 타입은 Arrow의 timestamp 타입으로 매핑합니다.
		return arrow.FixedWidthTypes.Timestamp_ns
	case "dense_vector":
		// Dense vector 타입은 Arrow의 fixed-size list 타입으로 매핑합니다.
		if dims, ok := fieldProps["dims"].(float64); ok {
			return arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)
		}
		// dims가 지정되지 않은 경우 기본값으로 0을 사용
		return arrow.FixedSizeListOf(0, arrow.PrimitiveTypes.Float32)
	case "nested", "object":
		// Nested 또는 Object 타입은 재귀적으로 처리합니다.
		if properties, ok := fieldProps["properties"].(map[string]interface{}); ok {
			return arrow.StructOf(parseProperties(properties)...)
		}
		return arrow.StructOf()
	default:
		return arrow.BinaryTypes.String
	}
}

func generateDummyData(properties map[string]interface{}, count int) []map[string]interface{} {
	data := make([]map[string]interface{}, count)
	for i := 0; i < count; i++ {
		data[i] = generateDocument(properties)
	}
	return data
}

func generateDocument(properties map[string]interface{}) map[string]interface{} {
	doc := make(map[string]interface{})
	for fieldName, fieldProps := range properties {
		props := fieldProps.(map[string]interface{})
		fieldType, _ := props["type"].(string)

		// 랜덤하게 리스트로 생성 (20% 확률)
		if rand.Float32() < 0.2 {
			doc[fieldName] = []interface{}{generateValue(fieldType, props)}
		} else {
			doc[fieldName] = generateValue(fieldType, props)
		}
	}
	return doc
}

func generateValue(fieldType string, props map[string]interface{}) interface{} {
	switch fieldType {
	case "text", "keyword":
		return fmt.Sprintf("dummy_%d", rand.Intn(1000))
	case "integer":
		return int32(rand.Intn(1000))
	case "long":
		return int64(rand.Int63())
	case "float":
		return rand.Float32()
	case "double":
		return rand.Float64()
	case "boolean":
		return rand.Intn(2) == 1
	case "date":
		return time.Now()
	case "nested", "object":
		if nestedProps, ok := props["properties"].(map[string]interface{}); ok {
			return generateDocument(nestedProps)
		}
	}
	return nil
}

func adjustField(field arrow.Field, value interface{}) arrow.Field {
	if value == nil {
		return field
	}

	switch v := value.(type) {
	case []interface{}, []string, []float32, []float64, []int, []int32, []int64:
		var elementType arrow.DataType
		switch field.Type.(type) {
		case *arrow.StringType:
			elementType = arrow.BinaryTypes.String
		case *arrow.Float32Type:
			elementType = arrow.PrimitiveTypes.Float32
		case *arrow.Float64Type:
			elementType = arrow.PrimitiveTypes.Float64
		case *arrow.Int32Type:
			elementType = arrow.PrimitiveTypes.Int32
		case *arrow.Int64Type:
			elementType = arrow.PrimitiveTypes.Int64
		default:
			elementType = field.Type
		}
		return arrow.Field{Name: field.Name, Type: arrow.ListOf(elementType), Nullable: true}
	case map[string]interface{}:
		if structType, ok := field.Type.(*arrow.StructType); ok {
			fields := make([]arrow.Field, 0, len(structType.Fields()))
			for _, f := range structType.Fields() {
				fields = append(fields, adjustField(f, v[f.Name]))
			}
			return arrow.Field{Name: field.Name, Type: arrow.StructOf(fields...), Nullable: true}
		}
	}
	return field
}

func adjustSchemaForLists(schema *arrow.Schema, data []map[string]interface{}) *arrow.Schema {
	adjustedFields := make([]arrow.Field, len(schema.Fields()))
	for i, field := range schema.Fields() {
		adjustedField := field
		for _, doc := range data {
			if value, ok := doc[field.Name]; ok {
				if nestedStruct, ok := value.(map[string]interface{}); ok && field.Type.ID() == arrow.STRUCT {
					structType := field.Type.(*arrow.StructType)
					fields := make([]arrow.Field, 0, len(structType.Fields()))
					for _, f := range structType.Fields() {
						fields = append(fields, adjustField(f, nestedStruct[f.Name]))
					}
					adjustedField = arrow.Field{Name: field.Name, Type: arrow.StructOf(fields...), Nullable: true}
				} else {
					adjustedField = adjustField(adjustedField, value)
				}
				break
			}
		}
		adjustedFields[i] = adjustedField
	}

	return arrow.NewSchema(adjustedFields, nil)
}

func createArrowRecord(schema *arrow.Schema, data []map[string]interface{}) arrow.Record {
	builders := make([]array.Builder, len(schema.Fields()))
	for i, field := range schema.Fields() {
		builders[i] = array.NewBuilder(memory.DefaultAllocator, field.Type)
	}

	for _, doc := range data {
		for i, field := range schema.Fields() {
			value := doc[field.Name]
			fmt.Printf("Field: %s, Value: %v, Type: %T\n", field.Name, value, value)
			appendValue(builders[i], value, schema)
		}
	}

	columns := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		columns[i] = builder.NewArray()
	}

	return array.NewRecord(schema, columns, int64(len(data)))
}

func appendValue(builder array.Builder, value interface{}, schema *arrow.Schema) {
	if value == nil {
		builder.AppendNull()
		return
	}

	switch b := builder.(type) {
	case *array.Int32Builder:
		switch v := value.(type) {
		case int32:
			b.Append(v)
		case int:
			b.Append(int32(v))
		case float64:
			b.Append(int32(v))
		default:
			b.AppendNull()
		}
	case *array.Int64Builder:
		switch v := value.(type) {
		case int64:
			b.Append(v)
		case int:
			b.Append(int64(v))
		case float64:
			b.Append(int64(v))
		default:
			b.AppendNull()
		}
	case *array.Float32Builder:
		switch v := value.(type) {
		case float32:
			b.Append(v)
		case float64:
			b.Append(float32(v))
		default:
			b.AppendNull()
		}
	case *array.Float64Builder:
		switch v := value.(type) {
		case float64:
			b.Append(v)
		case float32:
			b.Append(float64(v))
		default:
			b.AppendNull()
		}
	case *array.StringBuilder:
		if v, ok := value.(string); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.BooleanBuilder:
		if v, ok := value.(bool); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.TimestampBuilder:
		switch v := value.(type) {
		case time.Time:
			b.Append(arrow.Timestamp(v.UnixNano()))
		case string:
			t, err := time.Parse(time.RFC3339, v)
			if err == nil {
				b.Append(arrow.Timestamp(t.UnixNano()))
			} else {
				b.AppendNull()
			}
		default:
			b.AppendNull()
		}
	case *array.StructBuilder:
		if v, ok := value.(map[string]interface{}); ok {
			b.Append(true)
			for j := 0; j < b.NumField(); j++ {
				fieldBuilder := b.FieldBuilder(j)
				fieldValue := v[b.Type().(*arrow.StructType).Field(j).Name]
				appendValue(fieldBuilder, fieldValue, schema)
			}
		} else {
			b.AppendNull()
		}
	case *array.ListBuilder:
		b.Append(true)
		switch v := value.(type) {
		case []interface{}:
			for _, item := range v {
				appendValue(b.ValueBuilder(), item, schema)
			}
		case []string:
			for _, item := range v {
				appendValue(b.ValueBuilder(), item, schema)
			}
		case []float32:
			for _, item := range v {
				appendValue(b.ValueBuilder(), item, schema)
			}
		default:
			// 단일 값을 리스트의 단일 요소로 처리
			appendValue(b.ValueBuilder(), value, schema)
		}
	case *array.FixedSizeListBuilder:
		listType := b.Type().(*arrow.FixedSizeListType)
		listSize := int(listType.Len())
		valueBuilder := b.ValueBuilder()

		switch v := value.(type) {
		case []float32:
			if len(v) == listSize {
				b.Append(true)
				for _, item := range v {
					valueBuilder.(*array.Float32Builder).Append(item)
				}
			} else {
				// 길이가 맞지 않으면 null로 처리
				b.AppendNull()
			}
		case []float64:
			if len(v) == listSize {
				b.Append(true)
				for _, item := range v {
					valueBuilder.(*array.Float32Builder).Append(float32(item))
				}
			} else {
				b.AppendNull()
			}
		default:
			// 다른 타입이거나 단일 값인 경우 null로 처리
			b.AppendNull()
		}
	default:
		builder.AppendNull()
	}
}
