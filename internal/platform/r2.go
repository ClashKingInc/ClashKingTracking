package platform

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

type ObjectStore interface {
	PutObject(ctx context.Context, key string, payload []byte, contentType string) error
}

type MockObjectStore struct{}

func (MockObjectStore) PutObject(context.Context, string, []byte, string) error {
	return nil
}

type R2ObjectStore struct {
	endpoint        *url.URL
	accessKeyID     string
	secretAccessKey string
	bucket          string
	client          *http.Client
	now             func() time.Time
	limiter         *requestRateLimiter
}

func NewR2ObjectStore(cfg Config) (*R2ObjectStore, error) {
	if cfg.R2Endpoint == "" || cfg.R2AccessKeyID == "" || cfg.R2SecretAccessKey == "" || cfg.R2Bucket == "" {
		return nil, errors.New("r2 endpoint/bucket and R2 credentials are required when R2 is configured")
	}
	endpoint, err := url.Parse(strings.TrimRight(cfg.R2Endpoint, "/"))
	if err != nil {
		return nil, err
	}
	if endpoint.Scheme == "" || endpoint.Host == "" {
		return nil, fmt.Errorf("r2 endpoint must include scheme and host: %q", cfg.R2Endpoint)
	}
	return &R2ObjectStore{
		endpoint:        endpoint,
		accessKeyID:     cfg.R2AccessKeyID,
		secretAccessKey: cfg.R2SecretAccessKey,
		bucket:          strings.Trim(cfg.R2Bucket, "/"),
		client:          newR2HTTPClient(cfg.R2RequestsPerSecond),
		now:             time.Now,
		limiter:         newRequestRateLimiter(cfg.R2RequestsPerSecond),
	}, nil
}

func (s *R2ObjectStore) PutObject(ctx context.Context, key string, payload []byte, contentType string) error {
	if s == nil {
		return errors.New("r2 object store is not configured")
	}
	if s.limiter != nil {
		if err := s.limiter.Wait(ctx); err != nil {
			return err
		}
	}
	key = strings.TrimLeft(key, "/")
	target := *s.endpoint
	target.Path = "/" + s.bucket + "/" + key
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, target.String(), bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Content-Length", fmt.Sprintf("%d", len(payload)))
	if err := s.sign(req, payload); err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("r2 put %s failed: status=%d body=%s", key, resp.StatusCode, strings.TrimSpace(string(body)))
}

func newR2HTTPClient(requestsPerSecond int) *http.Client {
	maxConns := max(1, requestsPerSecond*3)
	return &http.Client{
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			MaxIdleConns:          maxConns,
			MaxIdleConnsPerHost:   maxConns,
			MaxConnsPerHost:       maxConns,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
		Timeout: 30 * time.Second,
	}
}

type requestRateLimiter struct {
	mu       sync.Mutex
	interval time.Duration
	next     time.Time
}

func newRequestRateLimiter(requestsPerSecond int) *requestRateLimiter {
	if requestsPerSecond <= 0 {
		return nil
	}
	return &requestRateLimiter{interval: time.Second / time.Duration(requestsPerSecond)}
}

func (l *requestRateLimiter) Wait(ctx context.Context) error {
	if l == nil || l.interval <= 0 {
		return nil
	}
	now := time.Now()
	l.mu.Lock()
	if l.next.Before(now) {
		l.next = now
	}
	waitUntil := l.next
	l.next = l.next.Add(l.interval)
	l.mu.Unlock()
	wait := time.Until(waitUntil)
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *R2ObjectStore) sign(req *http.Request, payload []byte) error {
	now := s.now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	payloadHash := sha256Hex(payload)
	req.Header.Set("Host", req.URL.Host)
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	signedHeaders := canonicalSignedHeaders(req.Header)
	canonicalRequest := strings.Join([]string{
		req.Method,
		req.URL.EscapedPath(),
		req.URL.RawQuery,
		canonicalHeaders(req.Header, signedHeaders),
		strings.Join(signedHeaders, ";"),
		payloadHash,
	}, "\n")

	scope := dateStamp + "/auto/s3/aws4_request"
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")
	signature := hex.EncodeToString(hmacSHA256(signingKey(s.secretAccessKey, dateStamp), []byte(stringToSign)))
	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		s.accessKeyID,
		scope,
		strings.Join(signedHeaders, ";"),
		signature,
	))
	return nil
}

func canonicalSignedHeaders(headers http.Header) []string {
	out := make([]string, 0, len(headers))
	for key := range headers {
		out = append(out, strings.ToLower(key))
	}
	sort.Strings(out)
	return out
}

func canonicalHeaders(headers http.Header, signed []string) string {
	var b strings.Builder
	for _, key := range signed {
		values := headers.Values(key)
		if len(values) == 0 {
			values = headers.Values(http.CanonicalHeaderKey(key))
		}
		for i := range values {
			values[i] = strings.Join(strings.Fields(values[i]), " ")
		}
		sort.Strings(values)
		b.WriteString(key)
		b.WriteByte(':')
		b.WriteString(strings.Join(values, ","))
		b.WriteByte('\n')
	}
	return b.String()
}

func signingKey(secret, dateStamp string) []byte {
	dateKey := hmacSHA256([]byte("AWS4"+secret), []byte(dateStamp))
	regionKey := hmacSHA256(dateKey, []byte("auto"))
	serviceKey := hmacSHA256(regionKey, []byte("s3"))
	return hmacSHA256(serviceKey, []byte("aws4_request"))
}

func hmacSHA256(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
