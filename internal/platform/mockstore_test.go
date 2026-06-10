//go:build platform_internal_tests

package platform

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMockCollectionDistinctStrings(t *testing.T) {
	store := NewMockStore()
	collection := store.Static().Collection("user_settings").(*mockCollection)
	// Mirror the nested shape used by user settings lookups so dotted field traversal is exercised.
	collection.docs = []bson.M{
		{"search": bson.M{"player": bson.M{"bookmarked": "#AAA"}}},
		{"search": bson.M{"player": bson.M{"bookmarked": "#BBB"}}},
		{"search": bson.M{"player": bson.M{"bookmarked": "#AAA"}}},
	}

	got, err := collection.DistinctStrings(context.Background(), "search.player.bookmarked", bson.M{})
	if err != nil {
		t.Fatalf("DistinctStrings returned error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 distinct values, got %d (%v)", len(got), got)
	}
}
