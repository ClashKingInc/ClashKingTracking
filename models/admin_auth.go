package models

import "time"

type AdminUser struct {
	ID            string     `json:"id"`
	DiscordUserID string     `json:"discord_user_id"`
	Username      string     `json:"username"`
	DisplayName   string     `json:"display_name"`
	AvatarURL     string     `json:"avatar_url,omitempty"`
	Role          string     `json:"role"`
	Active        bool       `json:"active"`
	LastLoginAt   *time.Time `json:"last_login_at,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

type AdminSession struct {
	ID        string    `json:"id"`
	User      AdminUser `json:"user"`
	ExpiresAt time.Time `json:"expires_at"`
}

type DiscordAdminProfile struct {
	ID          string
	Username    string
	DisplayName string
	AvatarURL   string
}
