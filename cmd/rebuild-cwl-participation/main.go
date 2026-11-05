package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"clashking_tracking/internal/wararchive"
	"clashking_tracking/scripts"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/klauspost/compress/zstd"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	_ = godotenv.Load()
	season := flag.String("season", "", "CWL season YYYY-MM (default latest stored)")
	archiveOrigin := flag.String("archive-origin", os.Getenv("WAR_ARCHIVE_ORIGIN"), "local war archive HTTP origin; omit to leave hit rates unavailable")
	flag.Parse()
	dsn := os.Getenv("TIMESCALE_URL")
	if strings.TrimSpace(os.Getenv("TIMESCALE_HOST")) != "" {
		uri := &url.URL{Scheme: "postgres", User: url.UserPassword(os.Getenv("TIMESCALE_USERNAME"), os.Getenv("TIMESCALE_PASSWORD")), Host: net.JoinHostPort(os.Getenv("TIMESCALE_HOST"), os.Getenv("TIMESCALE_PORT")), Path: "/" + os.Getenv("TIMESCALE_DATABASE")}
		q := url.Values{}
		q.Set("sslmode", os.Getenv("TIMESCALE_SSLMODE"))
		uri.RawQuery = q.Encode()
		dsn = uri.String()
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme != "postgres" || u.Port() != "54330" || u.Path != "/clashking_dev" || (u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1" && u.Hostname() != "::1") {
		return fmt.Errorf("refusing database outside local 127.0.0.1:54330/clashking_dev")
	}
	if u.Fragment != "" || u.Opaque != "" {
		return fmt.Errorf("refusing PostgreSQL URL override")
	}
	for key := range u.Query() {
		if key != "sslmode" {
			return fmt.Errorf("refusing PostgreSQL URL query override %q", key)
		}
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	var reader scripts.CWLArchiveReader
	if *archiveOrigin != "" {
		u, err := url.Parse(*archiveOrigin)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || (u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1" && u.Hostname() != "::1") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
			return fmt.Errorf("archive origin must be a local HTTP origin")
		}
		client := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }}
		decoder, err := zstd.NewReader(nil, zstd.WithDecoderDicts(wararchive.Dictionary))
		if err != nil {
			return err
		}
		defer decoder.Close()
		reader = func(ctx context.Context, loc scripts.CWLArchiveLocator) (wararchive.War, error) {
			key := wararchive.ObjectKey(loc.PackID)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(*archiveOrigin, "/")+"/"+key, nil)
			if err != nil {
				return wararchive.War{}, err
			}
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", loc.Offset, loc.Offset+int64(loc.Bytes)-1))
			resp, err := client.Do(req)
			if err != nil {
				return wararchive.War{}, err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusPartialContent {
				return wararchive.War{}, fmt.Errorf("archive %s returned HTTP %d", key, resp.StatusCode)
			}
			frame, err := io.ReadAll(io.LimitReader(resp.Body, int64(loc.Bytes)+1))
			if err != nil {
				return wararchive.War{}, err
			}
			if len(frame) != loc.Bytes {
				return wararchive.War{}, fmt.Errorf("archive %s range length mismatch", key)
			}
			raw, err := decoder.DecodeAll(frame, nil)
			if err != nil {
				return wararchive.War{}, err
			}
			return wararchive.Unmarshal(raw)
		}
	}
	report, err := scripts.RebuildCWLParticipation(context.Background(), pool, *season, reader)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(report)
}
