package main

import (
	"testing"
	"time"
)

func TestLocalDatabaseGuard(t *testing.T) {
	for _, dsn := range []string{
		"postgres://user:secret@db.example.com:54330/clashking_dev",
		"postgres://user:secret@127.0.0.1:5432/clashking_dev",
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

func TestCompletedDayAndDuplicateGuards(t *testing.T) {
	days, err := parseDays("2026-09-20")
	if err != nil {
		t.Fatal(err)
	}
	if err := requireCompletedDays(days, time.Date(2026, 9, 21, 5, 10, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if err := requireCompletedDays(days, time.Date(2026, 9, 21, 5, 9, 59, 0, time.UTC)); err == nil {
		t.Fatal("open Legend day accepted")
	}
	if _, err := parseDays("2026-09-20,2026-09-20"); err == nil {
		t.Fatal("duplicate day accepted")
	}
}
