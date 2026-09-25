package main

import (
	"testing"
	"time"
)

func TestRequireLocalDevDatabase(t *testing.T) {
	for _, dsn := range []string{
		"postgres://user:secret@db.example.com:54330/clashking",
		"postgres://user:secret@127.0.0.1:5432/clashking",
		"postgres://user:secret@127.0.0.1:54330/clashking",
		"postgres://user:secret@127.0.0.1:54330/clashking_dev?host=db.example.com",
	} {
		if requireLocalDevDatabase(dsn) == nil {
			t.Fatalf("unsafe DSN accepted: %s", dsn)
		}
	}
	if err := requireLocalDevDatabase("postgres://user:secret@127.0.0.1:54330/clashking_dev"); err != nil {
		t.Fatal(err)
	}
}

func TestParseDays(t *testing.T) {
	days, err := parseDays("2026-09-15,2026-09-20")
	if err != nil || len(days) != 2 {
		t.Fatalf("days=%v err=%v", days, err)
	}
	if _, err = parseDays("2026-09-15,2026-09-15"); err == nil {
		t.Fatal("duplicate day accepted")
	}
}

func TestRequireCompletedDaysUsesLegendResetBoundary(t *testing.T) {
	day20, _ := time.Parse("2006-01-02", "2026-09-20")
	if err := requireCompletedDays([]time.Time{day20}, time.Date(2026, 9, 21, 5, 10, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if err := requireCompletedDays([]time.Time{day20}, time.Date(2026, 9, 21, 5, 9, 59, 0, time.UTC)); err == nil {
		t.Fatal("open Legend day accepted before reset")
	}
}
