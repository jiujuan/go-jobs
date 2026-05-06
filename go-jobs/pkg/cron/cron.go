// Package cron provides a thin convenience wrapper around robfig/cron/v3.
package cron

import (
	"fmt"
	"time"

	cronv3 "github.com/robfig/cron/v3"
)

// SecondParser is a cron parser that supports a leading seconds field,
// matching the format used by xxl-job: "0/30 * * * * ?"
var SecondParser = cronv3.NewParser(
	cronv3.Second | cronv3.Minute | cronv3.Hour |
		cronv3.Dom | cronv3.Month | cronv3.Dow,
)

// NextTime returns the next scheduled time after `from` for the given cron expression.
func NextTime(expr string, from time.Time) (time.Time, error) {
	sched, err := SecondParser.Parse(expr)
	if err != nil {
		return time.Time{}, fmt.Errorf("cron: parse %q: %w", expr, err)
	}
	return sched.Next(from), nil
}

// IsValid returns true if expr is a valid cron expression.
func IsValid(expr string) bool {
	_, err := SecondParser.Parse(expr)
	return err == nil
}
