// Manual CWL summary publication. This command is not a scheduled tracker.
package main

import (
	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/wararchive"
	"clashking_tracking/scripts"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/klauspost/compress/zstd"
	"io"
	"os"
	"strings"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	seasons := flag.String("seasons", "", "explicit comma-separated YYYY-MM seasons")
	apply := flag.Bool("apply", false, "publish the specified seasons to the configured database")
	flag.Parse()
	if *seasons == "" {
		return fmt.Errorf("explicit seasons required")
	}
	labels := strings.Split(*seasons, ",")
	for _, s := range labels {
		v, e := scripts.CWLSeasonLabel(s)
		if e != nil || v != s {
			return fmt.Errorf("invalid season %q", s)
		}
	}
	if !*apply {
		fmt.Printf("planned seasons=%s; no writes (use --apply)\n", *seasons)
		return nil
	}
	cfg := platform.LoadWithArgs(nil)
	if cfg.TimescaleURL == "" || cfg.WarArchiveEndpoint == "" || cfg.WarArchiveAccessKeyID == "" {
		return fmt.Errorf("database and archive configuration required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()
	pc, err := pgxpool.ParseConfig(cfg.TimescaleURL)
	if err != nil {
		return err
	}
	pc.MaxConns = 1
	// Backfills are bounded operator jobs, not interactive requests. Avoid the
	// interactive 15s timeout while retaining a finite per-statement ceiling.
	pc.ConnConfig.RuntimeParams["statement_timeout"] = "900000"
	pc.ConnConfig.RuntimeParams["application_name"] = "cwl-participation-backfill"
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return err
	}
	defer pool.Close()
	client := s3.NewFromConfig(aws.Config{Region: "auto", Credentials: credentials.NewStaticCredentialsProvider(cfg.WarArchiveAccessKeyID, cfg.WarArchiveSecretAccessKey, "")}, func(o *s3.Options) { o.BaseEndpoint = aws.String(cfg.WarArchiveEndpoint); o.UsePathStyle = true })
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderDicts(wararchive.Dictionary), zstd.WithDecoderMaxMemory(32<<20))
	if err != nil {
		return err
	}
	defer decoder.Close()
	reader := func(ctx context.Context, loc scripts.CWLArchiveLocator) (wararchive.War, error) {
		requestCtx, stop := context.WithTimeout(ctx, 30*time.Second)
		defer stop()
		result, e := client.GetObject(requestCtx, &s3.GetObjectInput{Bucket: aws.String(cfg.WarArchiveBucket), Key: aws.String(wararchive.ObjectKey(loc.PackID)), Range: aws.String(fmt.Sprintf("bytes=%d-%d", loc.Offset, loc.Offset+int64(loc.Bytes)-1))})
		if e != nil {
			return wararchive.War{}, e
		}
		defer result.Body.Close()
		frame, e := io.ReadAll(io.LimitReader(result.Body, int64(loc.Bytes)+1))
		if e != nil {
			return wararchive.War{}, e
		}
		if len(frame) != loc.Bytes {
			return wararchive.War{}, fmt.Errorf("incorrect archive range length")
		}
		raw, e := decoder.DecodeAll(frame, nil)
		if e != nil {
			return wararchive.War{}, e
		}
		return wararchive.Unmarshal(raw)
	}
	for _, season := range labels {
		start := time.Now()
		fmt.Printf("cwl %s started=%s\n", season, start.UTC().Format(time.RFC3339))
		report, e := scripts.RebuildCWLParticipation(ctx, pool, season, reader)
		if e != nil {
			return fmt.Errorf("%s: %w", season, e)
		}
		json.NewEncoder(os.Stdout).Encode(struct {
			Season  string                         `json:"season"`
			Seconds float64                        `json:"seconds"`
			Report  scripts.CWLParticipationReport `json:"report"`
		}{season, time.Since(start).Seconds(), report})
	}
	return nil
}
