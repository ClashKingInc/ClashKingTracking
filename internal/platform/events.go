package platform

import "time"

type Event struct {
	Topic     string                 `json:"topic"`
	ClanTag   string                 `json:"clan_tag,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
	Value     map[string]interface{} `json:"value"`
}

type Filter struct {
	Topics map[string]struct{}
	Clans  map[string]struct{}
}
