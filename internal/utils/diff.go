package utils

import (
	"reflect"
	"strings"
)

func BSONTaggedFields(oldValue, newValue any, prefix string) map[string]any {
	updates := map[string]any{}
	oldReflect := reflect.Indirect(reflect.ValueOf(oldValue))
	newReflect := reflect.Indirect(reflect.ValueOf(newValue))
	// Mismatched or nil inputs are treated as "no diff" so callers can skip special-case guards.
	if !oldReflect.IsValid() || !newReflect.IsValid() || oldReflect.Type() != newReflect.Type() {
		return updates
	}

	typ := newReflect.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag := strings.Split(field.Tag.Get("bson"), ",")[0]
		if tag == "" || tag == "-" {
			continue
		}
		oldField := oldReflect.Field(i).Interface()
		newField := newReflect.Field(i).Interface()
		if !reflect.DeepEqual(oldField, newField) {
			updates[prefix+tag] = newField
		}
	}
	return updates
}
