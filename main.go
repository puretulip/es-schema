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

	// Arrow Schema 정의
	fields := []arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "hobbies", Type: arrow.StructOf(
			arrow.Field{Name: "activity", Type: arrow.BinaryTypes.String},
		)},
	}
	schema := arrow.NewSchema(fields, nil)

	// Arrow Schema 출력
	fmt.Println("Arrow Schema:")
	fmt.Println(schema)

	// 메모리 풀 생성
	pool := memory.NewGoAllocator()

	// 예제 데이터 생성
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.StringBuilder).Append("John Doe")
	hobbiesBuilder := b.Field(1).(*array.StructBuilder)
	hobbiesBuilder.Append(true)
	activityBuilder := hobbiesBuilder.FieldBuilder(0).(*array.StringBuilder)
	activityBuilder.Append("Reading")

	record := b.NewRecord()
	defer record.Release()

	fmt.Println("Record:")
	fmt.Println(record)
}
