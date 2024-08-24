package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/apache/arrow/go/v10/arrow"
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
                        "zipcode": { "type": "integer" },
                        "geo": {
                            "type": "nested",
                            "properties": {
                                "lat": { "type": "float" },
                                "lon": { "type": "float" }
                            }
                        }
                    }
                }
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
	schema := arrow.NewSchema(fields, nil)
	fmt.Println(schema)
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
		return arrow.FixedWidthTypes.Timestamp_ms
	case "dense_vector":
		// Dense vector 타입은 Arrow의 fixed-size list 타입으로 매핑합니다.
		if dims, ok := fieldProps["dims"].(float64); ok {
			return arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)
		}
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
