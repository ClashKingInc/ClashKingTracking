package scripts

import (
	"bytes"
	"encoding/json"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func jsonBytes(value any) []byte {
	raw, _ := json.Marshal(value)
	return raw
}

func jsonDocument(value any) bson.M {
	return rawDocument(jsonBytes(value))
}

func jsonValue(value any) any {
	return rawValue(jsonBytes(value))
}

func rawDocument(raw []byte) bson.M {
	var doc bson.M
	_ = json.Unmarshal(raw, &doc)
	return doc
}

func rawValue(raw []byte) any {
	var value any
	_ = json.Unmarshal(raw, &value)
	return value
}

func equalJSONBytes(left, right []byte) bool {
	return bytes.Equal(left, right)
}
