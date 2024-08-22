package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
)

func main() {
	// JSON 매핑 테이블
	mapping := `{
        "properties": {
            "hobbies": {
                "properties": {
                    "activity": { "type": "text" }
                },
                "type": "nested"
            },
            "name": { "type": "text" }
        }
    }`

	// JSON 파싱
	var esMapping map[string]interface{}
	err := json.Unmarshal([]byte(mapping), &esMapping)
	if err != nil {
		log.Fatalf("Error parsing JSON: %v", err)
	}

	// Arrow 스키마 생성
	schema := createArrowSchema(esMapping["properties"].(map[string]interface{}))

	// Arrow 스키마 출력
	fmt.Println("Arrow Schema:")
	fmt.Println(schema)

	// 데이터 예제
	data := []map[string]interface{}{
		{
			"name":    "Bob",
			"hobbies": map[string]string{"activity": "cycling"}, // 리스트가 아닌 단일 값
		},
		{
			"name":    "Alice",
			"hobbies": []map[string]string{{"activity": "reading books"}, {"activity": "playing piano"}},
		},
		{
			"name":    []string{"Charlie", "Charles"}, // name 필드가 리스트인 경우
			"hobbies": []map[string]string{{"activity": "swimming"}, {"activity": "running"}},
		},
	}

	// 필드 이름과 인덱스를 매핑
	fieldIndex := make(map[string]int)
	for i, field := range schema.Fields() {
		fieldIndex[field.Name] = i
	}

	// 메모리 풀 생성
	pool := memory.NewGoAllocator()

	// 예제 데이터 생성
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	for _, record := range data {
		for key, value := range record {
			appendValue(b, fieldIndex, key, value)
		}
	}

	record := b.NewRecord()
	defer record.Release()

	fmt.Println("Record:")
	fmt.Println(record)
}

func createArrowSchema(properties map[string]interface{}) *arrow.Schema {
	var fields []arrow.Field
	for key, val := range properties {
		fieldType := val.(map[string]interface{})
		fields = append(fields, createArrowField(key, fieldType))
	}
	return arrow.NewSchema(fields, nil)
}

func createArrowField(name string, fieldType map[string]interface{}) arrow.Field {
	switch fieldType["type"] {
	case "text":
		return arrow.Field{Name: name, Type: arrow.BinaryTypes.String}
	case "nested":
		nestedProperties := fieldType["properties"].(map[string]interface{})
		var nestedFields []arrow.Field
		for nestedKey, nestedVal := range nestedProperties {
			nestedFieldType := nestedVal.(map[string]interface{})
			nestedFields = append(nestedFields, createArrowField(nestedKey, nestedFieldType))
		}
		return arrow.Field{Name: name, Type: arrow.StructOf(nestedFields...)}
	default:
		return arrow.Field{Name: name, Type: arrow.BinaryTypes.String}
	}
}

func appendValue(b *array.RecordBuilder, fieldIndex map[string]int, key string, value interface{}) {
	switch v := value.(type) {
	case string:
		b.Field(fieldIndex[key]).(*array.StringBuilder).Append(v)
	case []string:
		listBuilder := b.Field(fieldIndex[key]).(*array.ListBuilder)
		listBuilder.Append(true)
		strBuilder := listBuilder.ValueBuilder().(*array.StringBuilder)
		for _, s := range v {
			strBuilder.Append(s)
		}
	case map[string]string:
		structBuilder := b.Field(fieldIndex[key]).(*array.StructBuilder)
		structBuilder.Append(true)
		for k, val := range v {
			appendStructValue(structBuilder, k, val)
		}
	case []map[string]string:
		listBuilder := b.Field(fieldIndex[key]).(*array.ListBuilder)
		listBuilder.Append(true)
		structBuilder := listBuilder.ValueBuilder().(*array.StructBuilder)
		for _, m := range v {
			structBuilder.Append(true)
			for k, val := range m {
				appendStructValue(structBuilder, k, val)
			}
		}
	}
}

func appendStructValue(structBuilder *array.StructBuilder, key string, value interface{}) {
	structType := structBuilder.Type().(*arrow.StructType)
	fieldIndex := make(map[string]int)
	for i, field := range structType.Fields() {
		fieldIndex[field.Name] = i
	}

	switch v := value.(type) {
	case string:
		structBuilder.FieldBuilder(fieldIndex[key]).(*array.StringBuilder).Append(v)
	}
}
