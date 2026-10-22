package records_test

import (
	"testing"
	"time"

	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
)

func TestWindowContains(t *testing.T) {
	from := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)

	for _, tc := range []struct {
		name   string
		window records.Window
		at     time.Time
		want   bool
	}{
		{"no bounds keeps everything", records.Window{}, from.Add(-24 * time.Hour), true},
		{"the start is inclusive", records.Window{FromTimestamp: &from}, from, true},
		{"before the start is outside", records.Window{FromTimestamp: &from}, from.Add(-time.Millisecond), false},
		{"the end is exclusive", records.Window{ToTimestamp: &to}, to, false},
		{"before the end is inside", records.Window{ToTimestamp: &to}, to.Add(-time.Millisecond), true},
		{"between both bounds is inside", records.Window{FromTimestamp: &from, ToTimestamp: &to}, from.Add(time.Minute), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.window.Contains(tc.at); got != tc.want {
				t.Fatalf("Contains(%v) = %v, want %v", tc.at, got, tc.want)
			}
		})
	}
}
