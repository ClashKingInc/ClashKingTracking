package main

import (
	"reflect"
	"testing"
	"time"
)

func TestSelectedSeasons(t *testing.T) {
	original := timeNow
	t.Cleanup(func() { timeNow = original })
	timeNow = func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }
	if got, err := selectedSeasons("all"); err != nil || got != nil {
		t.Fatalf("all = %#v, %v", got, err)
	}
	want := []string{"2026-01", "2025-12"}
	if got, err := selectedSeasons("current_previous"); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("current_previous = %#v, %v; want %#v", got, err, want)
	}
	if _, err := selectedSeasons("current"); err == nil {
		t.Fatal("unsupported scope accepted")
	}
}
