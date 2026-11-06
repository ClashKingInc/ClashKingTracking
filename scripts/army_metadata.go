package scripts

// armyMetadata is the read-only part of static data used by army analytics.
// Clashy's public lookup returns a deep copy, including all upgrade levels.
// Closeouts must not repeat that copy for every item in every observed attack.
type armyMetadata interface {
	LookupByID(int) map[string]any
}

type armyMetadataCache struct {
	source armyMetadata
	items  map[int]map[string]any
}

// A cache belongs to one sequential closeout stage. Its values are private
// copies from the public client; analytics must only read them. Do not share
// this mutable cache between workers or expose its maps to callers.
func cacheArmyMetadata(source armyMetadata) armyMetadata {
	if _, ok := source.(*armyMetadataCache); ok {
		return source
	}
	return &armyMetadataCache{source: source, items: make(map[int]map[string]any)}
}

func (c *armyMetadataCache) LookupByID(id int) map[string]any {
	if item, ok := c.items[id]; ok {
		return item
	}
	item := c.source.LookupByID(id)
	c.items[id] = item
	return item
}
