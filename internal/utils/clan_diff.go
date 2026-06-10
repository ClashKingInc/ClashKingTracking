package utils

func ClanFields(before, after any) map[string]any {
	return BSONTaggedFields(before, after, "data.")
}
