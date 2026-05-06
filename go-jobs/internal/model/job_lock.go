package model

import "time"

// JobLock maps to job_lock.
// Used as a DB-level distributed lock fallback when Redis is unavailable.
// The scheduler atomically INSERT-or-fails on lock_key (unique constraint).
type JobLock struct {
	ID         int64     `gorm:"primaryKey;autoIncrement"         json:"id"`
	LockKey    string    `gorm:"size:128;not null;uniqueIndex"    json:"lock_key"`
	LockUntil  time.Time `gorm:"not null"                         json:"lock_until"`
	LockNode   string    `gorm:"size:128;not null"                json:"lock_node"`
	CreateTime time.Time `gorm:"autoCreateTime"                   json:"create_time"`
}

func (JobLock) TableName() string { return "job_lock" }
