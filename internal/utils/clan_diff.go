package utils

import (
	"reflect"
	"strings"

	"clashking_tracking/models"
)

func ClanFields(before, after models.Clan) map[string]any {
	changes := make(map[string]any)
	t := reflect.TypeOf(after)
	bv := reflect.ValueOf(before)
	av := reflect.ValueOf(after)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		// The mongo update paths are derived from the struct's BSON tags, not Go field names.
		tag := strings.Split(field.Tag.Get("bson"), ",")[0]
		if tag == "" || tag == "-" {
			continue
		}
		if !reflect.DeepEqual(bv.Field(i).Interface(), av.Field(i).Interface()) {
			changes["data."+tag] = av.Field(i).Interface()
		}
	}
	return changes
}
