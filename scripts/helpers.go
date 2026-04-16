package scripts

import (
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func escapeTag(tag string) string {
	return strings.ReplaceAll(tag, "#", "%23")
}

func mergeStrings(a, b []string) []string {
	// Preserve first-seen order while deduplicating tags pulled from different stores.
	set := make(map[string]struct{}, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, value := range append(a, b...) {
		if value == "" {
			continue
		}
		if _, ok := set[value]; ok {
			continue
		}
		set[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func buildStringSet(values []string, err error) (map[string]struct{}, error) {
	if err != nil {
		return nil, err
	}
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out, nil
}

func decodeBSON(doc bson.M, out any) error {
	// Re-marshal through BSON so the shared model tags drive the final shape.
	raw, err := bson.Marshal(doc)
	if err != nil {
		return err
	}
	return bson.Unmarshal(raw, out)
}
