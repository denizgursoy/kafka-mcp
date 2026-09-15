package consumerlag

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

// Window is the message throughput over one period of time.
type Window struct {
	Window    string  `json:"window"`
	Messages  int64   `json:"messages"`
	PerSecond float64 `json:"per_second"`
	PerMinute float64 `json:"per_minute"`
	PerHour   float64 `json:"per_hour"`

	// WindowTruncated is true when the topic is younger than the window, so
	// the rate was divided by the topic's actual age instead. Without this a
	// minute of traffic in a minute-old topic would be reported as an hourly
	// rate sixty times too low.
	WindowTruncated bool    `json:"window_truncated,omitempty"`
	ActualSeconds   float64 `json:"actual_seconds,omitempty"`
}

// ProduceRate is the produce throughput over three real windows.
type ProduceRate struct {
	LastSecond Window `json:"last_second"`
	LastMinute Window `json:"last_minute"`
	LastHour   Window `json:"last_hour"`
	Note       string `json:"note"`
}

// SampledRate is a consume rate obtained by reading committed offsets twice.
type SampledRate struct {
	MessagesConsumed int64   `json:"messages_consumed"`
	SampledSeconds   int     `json:"sampled_seconds"`
	PerSecond        float64 `json:"per_second"`
	PerMinute        float64 `json:"per_minute"`
	PerHour          float64 `json:"per_hour"`

	// SampleInconclusive is true when nothing at all moved during the sample.
	// A consumer that is merely idle and one that is stuck look identical over
	// a few seconds, and the caller must not be told they are the same.
	SampleInconclusive bool   `json:"sample_inconclusive,omitempty"`
	Note               string `json:"note"`
}

// measureProduceRate reports how many messages arrived in the last second,
// minute and hour, using message timestamps rather than any sampling.
func measureProduceRate(
	ctx context.Context,
	admin *kadm.Client,
	topic string,
) (*ProduceRate, error) {

	ends, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets for %q: %w", topic, err)
	}

	if err := ends.Error(); err != nil {
		return nil, fmt.Errorf("list end offsets for %q: %w", topic, err)
	}

	starts, err := admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list start offsets for %q: %w", topic, err)
	}

	if err := starts.Error(); err != nil {
		return nil, fmt.Errorf("list start offsets for %q: %w", topic, err)
	}

	oldest, err := oldestTimestamp(ctx, admin, topic)
	if err != nil {
		return nil, err
	}

	now := time.Now()

	rate := &ProduceRate{
		Note: "measured from message timestamps, so these are real historical windows",
	}

	for _, spec := range []struct {
		name   string
		length time.Duration
		into   *Window
	}{
		{"last_second", time.Second, &rate.LastSecond},
		{"last_minute", time.Minute, &rate.LastMinute},
		{"last_hour", time.Hour, &rate.LastHour},
	} {
		window, err := produceWindow(ctx, admin, topic, spec.name, spec.length, ends, now, oldest)
		if err != nil {
			return nil, err
		}

		*spec.into = window
	}

	return rate, nil
}

// produceWindow counts the messages produced within one window by resolving
// the offset each partition held at the start of it.
func produceWindow(
	ctx context.Context,
	admin *kadm.Client,
	topic string,
	name string,
	length time.Duration,
	ends kadm.ListedOffsets,
	now time.Time,
	oldest *time.Time,
) (Window, error) {

	from := now.Add(-length)

	at, err := admin.ListOffsetsAfterMilli(ctx, from.UnixMilli(), topic)
	if err != nil {
		return Window{}, fmt.Errorf("resolve %s window for %q: %w", name, topic, err)
	}

	window := Window{Window: name}

	at.Each(func(offset kadm.ListedOffset) {
		if offset.Err != nil {
			return
		}

		end, ok := ends.Lookup(topic, offset.Partition)
		if !ok || end.Err != nil {
			return
		}

		produced := end.Offset - offset.Offset
		if produced > 0 {
			window.Messages += produced
		}
	})

	seconds := length.Seconds()

	// A window longer than the topic's own history would spread the messages
	// over time that did not exist, so the real elapsed time is used instead.
	if oldest != nil && oldest.After(from) {
		actual := now.Sub(*oldest).Seconds()

		if actual > 0 && actual < seconds {
			seconds = actual
			window.WindowTruncated = true
			window.ActualSeconds = round(actual, 3)
		}
	}

	if seconds <= 0 {
		return window, nil
	}

	perSecond := float64(window.Messages) / seconds

	window.PerSecond = round(perSecond, 3)
	window.PerMinute = round(perSecond*60, 3)
	window.PerHour = round(perSecond*3600, 3)

	return window, nil
}

// oldestTimestamp returns the timestamp of the topic's earliest surviving
// message, or nil when the topic is empty.
func oldestTimestamp(
	ctx context.Context,
	admin *kadm.Client,
	topic string,
) (*time.Time, error) {

	listed, err := admin.ListOffsetsAfterMilli(ctx, 0, topic)
	if err != nil {
		return nil, fmt.Errorf("resolve oldest timestamp for %q: %w", topic, err)
	}

	var oldest *time.Time

	listed.Each(func(offset kadm.ListedOffset) {
		// A partition with no records reports a timestamp of -1, which is not
		// a point in time.
		if offset.Err != nil || offset.Timestamp < 0 {
			return
		}

		at := time.UnixMilli(offset.Timestamp).UTC()

		if oldest == nil || at.Before(*oldest) {
			oldest = &at
		}
	})

	return oldest, nil
}

// sampleConsumeRate reports how far the group's committed offsets advanced
// between the two readings.
func sampleConsumeRate(
	topic string,
	group string,
	committedBefore int64,
	after kadm.DescribedGroupLags,
	sampleSeconds int,
) *SampledRate {

	rate := &SampledRate{
		SampledSeconds: sampleSeconds,
		Note: fmt.Sprintf(
			"extrapolated from a %ds sample of committed offsets, not a historical average",
			sampleSeconds),
	}

	described, ok := after[group]
	if !ok || described.Error() != nil {
		rate.SampleInconclusive = true

		return rate
	}

	committedAfter := int64(0)

	for _, partition := range described.Lag[topic] {
		if partition.Err != nil {
			continue
		}

		committedAfter += partition.Commit.At
	}

	consumed := committedAfter - committedBefore

	// A negative move means the group was reset or rebalanced mid-sample, and
	// reporting that as negative throughput would be nonsense.
	if consumed < 0 {
		rate.SampleInconclusive = true

		return rate
	}

	rate.MessagesConsumed = consumed

	if sampleSeconds > 0 {
		perSecond := float64(consumed) / float64(sampleSeconds)

		rate.PerSecond = round(perSecond, 3)
		rate.PerMinute = round(perSecond*60, 3)
		rate.PerHour = round(perSecond*3600, 3)
	}

	if consumed == 0 {
		rate.SampleInconclusive = true
	}

	return rate
}

// estimate works out whether the lag is shrinking and, if so, when it clears.
func estimate(measured *GroupLag, produce *ProduceRate) {
	measured.Status = status(*measured, measured.ConsumeRate, produce)

	if measured.ConsumeRate == nil {
		return
	}

	// The backlog does not drain at the consume rate: producers keep adding to
	// it, so it drains at the difference between the two.
	producePerSecond := 0.0

	if produce != nil {
		producePerSecond = produce.LastMinute.PerSecond
	}

	drain := measured.ConsumeRate.PerSecond - producePerSecond
	rounded := round(drain, 3)
	measured.DrainPerSecond = &rounded

	switch measured.Status {
	case statusDraining:
		seconds := float64(measured.Lag) / drain
		rounded := round(seconds, 1)

		at := time.Now().Add(time.Duration(seconds * float64(time.Second))).UTC()

		measured.ETASeconds = &rounded
		measured.ETAHuman = humanDuration(seconds)
		measured.ETAAt = &at

	case statusGrowing:
		growth := round(-drain*60, 3)
		measured.GrowingPerMin = &growth
	}
}

// status names what the measurements mean, including every case where no
// completion time can honestly be given.
func status(measured GroupLag, consume *SampledRate, produce *ProduceRate) string {
	if measured.Lag == 0 {
		return statusCaughtUp
	}

	if consume == nil {
		return statusNotMeasured
	}

	// A group with no members has nothing to drain the backlog, whatever the
	// numbers say.
	if measured.Members == 0 {
		return statusNoConsumers
	}

	if consume.PerSecond == 0 {
		return statusStalled
	}

	producePerSecond := 0.0

	if produce != nil {
		producePerSecond = produce.LastMinute.PerSecond
	}

	if consume.PerSecond-producePerSecond <= 0 {
		return statusGrowing
	}

	return statusDraining
}

// humanDuration renders a number of seconds the way someone waiting for a
// backlog to clear would say it.
func humanDuration(seconds float64) string {
	if seconds < 1 {
		return "less than a second"
	}

	duration := time.Duration(seconds * float64(time.Second))

	days := int(duration.Hours()) / 24
	hours := int(duration.Hours()) % 24
	minutes := int(duration.Minutes()) % 60
	secs := int(duration.Seconds()) % 60

	parts := make([]string, 0, 4)

	if days > 0 {
		parts = append(parts, fmt.Sprintf("%dd", days))
	}

	if hours > 0 {
		parts = append(parts, fmt.Sprintf("%dh", hours))
	}

	if minutes > 0 {
		parts = append(parts, fmt.Sprintf("%dm", minutes))
	}

	if secs > 0 || len(parts) == 0 {
		parts = append(parts, fmt.Sprintf("%ds", secs))
	}

	return join(parts, " ")
}

func join(parts []string, sep string) string {
	out := ""

	for i, part := range parts {
		if i > 0 {
			out += sep
		}

		out += part
	}

	return out
}

// round keeps reported rates readable instead of carrying float noise.
func round(value float64, places int) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}

	factor := math.Pow(10, float64(places))

	return math.Round(value*factor) / factor
}
