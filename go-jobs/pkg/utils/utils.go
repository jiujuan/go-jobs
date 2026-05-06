// Package utils contains general-purpose helpers used across go-jobs.
package utils

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// ─── ID helpers ───────────────────────────────────────────────────────────────

// NewRequestID generates a random 16-byte hex string suitable as a request ID.
func NewRequestID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// ─── Network helpers ──────────────────────────────────────────────────────────

// LocalIP returns the first non-loopback IPv4 address of the host.
func LocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "127.0.0.1"
	}
	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				return ipNet.IP.String()
			}
		}
	}
	return "127.0.0.1"
}

// NodeID returns a stable identifier for this process: "ip:port".
func NodeID(port int) string {
	return fmt.Sprintf("%s:%d", LocalIP(), port)
}

// ─── String helpers ───────────────────────────────────────────────────────────

// SplitAndTrim splits s by sep and trims whitespace from each element.
func SplitAndTrim(s, sep string) []string {
	parts := strings.Split(s, sep)
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

// Int64SliceToString converts a []int64 to a comma-joined string.
func Int64SliceToString(ids []int64) string {
	strs := make([]string, len(ids))
	for i, id := range ids {
		strs[i] = strconv.FormatInt(id, 10)
	}
	return strings.Join(strs, ",")
}

// StringToInt64Slice parses a comma-joined string to []int64.
func StringToInt64Slice(s string) []int64 {
	if s == "" {
		return nil
	}
	parts := SplitAndTrim(s, ",")
	result := make([]int64, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.ParseInt(p, 10, 64)
		if err == nil {
			result = append(result, n)
		}
	}
	return result
}

// ─── Time helpers ─────────────────────────────────────────────────────────────

// FormatTime formats a time.Time to "2006-01-02 15:04:05".
func FormatTime(t time.Time) string {
	return t.Format("2006-01-02 15:04:05")
}

// ParseTime parses "2006-01-02 15:04:05" into a time.Time.
func ParseTime(s string) (time.Time, error) {
	return time.ParseInLocation("2006-01-02 15:04:05", s, time.Local)
}

// DurationMs returns the duration in milliseconds as an int64.
func DurationMs(start, end time.Time) int64 {
	return end.Sub(start).Milliseconds()
}

// ─── Environment helpers ──────────────────────────────────────────────────────

// EnvOrDefault returns the value of the env variable key, or defaultVal if not set.
func EnvOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

// EnvIntOrDefault returns the integer value of an env variable, or defaultVal.
func EnvIntOrDefault(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return defaultVal
}

// ─── Pagination helpers ───────────────────────────────────────────────────────

// PageOffset computes the SQL OFFSET from page (1-based) and pageSize.
func PageOffset(page, pageSize int) int {
	if page <= 0 {
		page = 1
	}
	return (page - 1) * pageSize
}

// SafePageSize clamps pageSize to [1, max].
func SafePageSize(pageSize, max int) int {
	if pageSize <= 0 {
		return 20
	}
	if pageSize > max {
		return max
	}
	return pageSize
}
