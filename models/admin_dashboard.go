package models

import "time"

// AdminDashboardSnapshot is the single operational read model used by the
// admin panel. Values are computed from the real device/content/delivery
// tables; the UI must never invent delivery health from campaign statuses.
type AdminDashboardSnapshot struct {
	GeneratedAt   time.Time                 `json:"generated_at"`
	Devices       AdminDeviceMetrics        `json:"devices"`
	Content       AdminContentMetrics       `json:"content"`
	Delivery      AdminDeliveryMetrics      `json:"delivery"`
	Daily         []AdminDeliveryDailyPoint `json:"daily"`
	AudienceDaily []AdminAudienceDailyPoint `json:"audience_daily"`
	AppVersions   []AdminDimensionCount     `json:"app_versions"`
	Locales       []AdminDimensionCount     `json:"locales"`
}

type AdminDeviceMetrics struct {
	Total      int `json:"total"`
	Production int `json:"production"`
	Sandbox    int `json:"sandbox"`
	Android    int `json:"android"`
	IOS        int `json:"ios"`
	Authorized int `json:"authorized"`
	OptedIn    int `json:"opted_in"`
	Active24H  int `json:"active_24h"`
	Active7D   int `json:"active_7d"`
}

type AdminDimensionCount struct {
	Value string `json:"value"`
	Count int    `json:"count"`
}

type AdminAudienceDailyPoint struct {
	Date       string `json:"date"`
	Total      int    `json:"total"`
	Production int    `json:"production"`
	Sandbox    int    `json:"sandbox"`
	OptedIn    int    `json:"opted_in"`
}

type AdminContentMetrics struct {
	LivePosts          int `json:"live_posts"`
	ScheduledPosts     int `json:"scheduled_posts"`
	DraftPosts         int `json:"draft_posts"`
	ScheduledCampaigns int `json:"scheduled_campaigns"`
	RecurringCampaigns int `json:"recurring_campaigns"`
}

type AdminDeliveryMetrics struct {
	Attempts    int        `json:"attempts"`
	Eligible    int        `json:"eligible"`
	Sent        int        `json:"sent"`
	Skipped     int        `json:"skipped"`
	Failed      int        `json:"failed"`
	SuccessRate float64    `json:"success_rate"`
	LastAttempt *time.Time `json:"last_attempt,omitempty"`
	NextSendAt  *time.Time `json:"next_send_at,omitempty"`
}

type AdminDeliveryDailyPoint struct {
	Date     string `json:"date"`
	Attempts int    `json:"attempts"`
	Eligible int    `json:"eligible"`
	Sent     int    `json:"sent"`
	Skipped  int    `json:"skipped"`
	Failed   int    `json:"failed"`
}
