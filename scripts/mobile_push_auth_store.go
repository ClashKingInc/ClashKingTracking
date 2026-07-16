package scripts

import (
	"context"
	"errors"
	"time"

	"clashking_tracking/models"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const adminUserColumns = `id, discord_user_id, username, display_name, avatar_url, role, active, last_login_at, created_at, updated_at`

func scanAdminUser(scanner interface{ Scan(...any) error }) (models.AdminUser, error) {
	var user models.AdminUser
	err := scanner.Scan(&user.ID, &user.DiscordUserID, &user.Username, &user.DisplayName, &user.AvatarURL,
		&user.Role, &user.Active, &user.LastLoginAt, &user.CreatedAt, &user.UpdatedAt)
	return user, err
}

func (s *timescaleMobilePushStore) UpsertDiscordAdminUser(ctx context.Context, profile models.DiscordAdminProfile) (models.AdminUser, error) {
	return scanAdminUser(s.pool.QueryRow(ctx, `INSERT INTO admin_users
		(discord_user_id, username, display_name, avatar_url, role, active)
		VALUES ($1,$2,$3,$4,'owner',true)
		ON CONFLICT (discord_user_id) DO UPDATE SET username=excluded.username,
			display_name=excluded.display_name, avatar_url=excluded.avatar_url, updated_at=now()
		RETURNING `+adminUserColumns, profile.ID, profile.Username, profile.DisplayName, profile.AvatarURL))
}

func (s *timescaleMobilePushStore) CreateAdminSession(ctx context.Context, userID, tokenHash, ipAddress, userAgent string, expiresAt time.Time) (models.AdminSession, error) {
	var session models.AdminSession
	err := s.pool.QueryRow(ctx, `INSERT INTO admin_sessions (user_id, token_hash, expires_at, ip_address, user_agent)
		VALUES ($1,$2,$3,$4,$5) RETURNING id, expires_at`, userID, tokenHash, expiresAt, ipAddress, userAgent).
		Scan(&session.ID, &session.ExpiresAt)
	if err != nil {
		return session, err
	}
	user, err := scanAdminUser(s.pool.QueryRow(ctx, `UPDATE admin_users SET last_login_at=now() WHERE id=$1 RETURNING `+adminUserColumns, userID))
	session.User = user
	return session, err
}

func (s *timescaleMobilePushStore) GetAdminSession(ctx context.Context, tokenHash string, now time.Time) (models.AdminSession, bool, error) {
	var session models.AdminSession
	err := s.pool.QueryRow(ctx, `SELECT s.id, s.expires_at, `+adminUserColumnsWithAlias("u")+`
		FROM admin_sessions s JOIN admin_users u ON u.id=s.user_id
		WHERE s.token_hash=$1 AND s.revoked_at IS NULL AND s.expires_at>$2 AND u.active=true`, tokenHash, now).
		Scan(&session.ID, &session.ExpiresAt, &session.User.ID, &session.User.DiscordUserID, &session.User.Username,
			&session.User.DisplayName, &session.User.AvatarURL, &session.User.Role, &session.User.Active,
			&session.User.LastLoginAt, &session.User.CreatedAt, &session.User.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return models.AdminSession{}, false, nil
	}
	if err == nil {
		_, _ = s.pool.Exec(ctx, `UPDATE admin_sessions SET last_seen_at=now() WHERE id=$1 AND last_seen_at < now()-interval '5 minutes'`, session.ID)
	}
	return session, err == nil, err
}

func adminUserColumnsWithAlias(alias string) string {
	return alias + `.id, ` + alias + `.discord_user_id, ` + alias + `.username, ` + alias + `.display_name, ` + alias + `.avatar_url, ` + alias + `.role, ` + alias + `.active, ` + alias + `.last_login_at, ` + alias + `.created_at, ` + alias + `.updated_at`
}

func (s *timescaleMobilePushStore) DeleteAdminSession(ctx context.Context, tokenHash string) error {
	_, err := s.pool.Exec(ctx, `UPDATE admin_sessions SET revoked_at=now() WHERE token_hash=$1 AND revoked_at IS NULL`, tokenHash)
	return err
}

func (s *timescaleMobilePushStore) DeleteAdminUserSessions(ctx context.Context, userID string) error {
	_, err := s.pool.Exec(ctx, `UPDATE admin_sessions SET revoked_at=now() WHERE user_id=$1 AND revoked_at IS NULL`, userID)
	return err
}

func (s *memoryMobilePushStore) UpsertDiscordAdminUser(_ context.Context, profile models.DiscordAdminProfile) (models.AdminUser, error) {
	for id, user := range s.adminUsers {
		if user.DiscordUserID == profile.ID {
			user.Username, user.DisplayName, user.AvatarURL, user.UpdatedAt = profile.Username, profile.DisplayName, profile.AvatarURL, time.Now().UTC()
			s.adminUsers[id] = user
			return user, nil
		}
	}
	now := time.Now().UTC()
	user := models.AdminUser{ID: uuid.NewString(), DiscordUserID: profile.ID, Username: profile.Username, DisplayName: profile.DisplayName, AvatarURL: profile.AvatarURL, Role: "owner", Active: true, CreatedAt: now, UpdatedAt: now}
	s.adminUsers[user.ID] = user
	return user, nil
}

func (s *memoryMobilePushStore) CreateAdminSession(_ context.Context, userID, tokenHash, _, _ string, expiresAt time.Time) (models.AdminSession, error) {
	user := s.adminUsers[userID]
	now := time.Now().UTC()
	user.LastLoginAt = &now
	s.adminUsers[userID] = user
	session := models.AdminSession{ID: uuid.NewString(), User: user, ExpiresAt: expiresAt}
	s.adminSessions[tokenHash] = session
	return session, nil
}

func (s *memoryMobilePushStore) GetAdminSession(_ context.Context, tokenHash string, now time.Time) (models.AdminSession, bool, error) {
	session, found := s.adminSessions[tokenHash]
	if !found || !session.ExpiresAt.After(now) {
		return models.AdminSession{}, false, nil
	}
	user, found := s.adminUsers[session.User.ID]
	if !found || !user.Active {
		return models.AdminSession{}, false, nil
	}
	session.User = user
	return session, true, nil
}

func (s *memoryMobilePushStore) DeleteAdminSession(_ context.Context, tokenHash string) error {
	delete(s.adminSessions, tokenHash)
	return nil
}
func (s *memoryMobilePushStore) DeleteAdminUserSessions(_ context.Context, userID string) error {
	for hash, session := range s.adminSessions {
		if session.User.ID == userID {
			delete(s.adminSessions, hash)
		}
	}
	return nil
}
