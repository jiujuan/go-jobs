// Package model defines all database models for go-jobs.
// GORM tags are used for column mapping; JSON tags for API serialisation.
package model

import "time"

// ExecutorStatus enumerates the online/offline states of an executor.
type ExecutorStatus int8

const (
	ExecutorOffline ExecutorStatus = 0
	ExecutorOnline  ExecutorStatus = 1
)

// RegisterType indicates how an executor was registered.
type RegisterType int8

const (
	RegisterAuto   RegisterType = 0 // executor auto-registers via heartbeat
	RegisterManual RegisterType = 1 // manually added in admin UI
)

// Executor maps to sys_executor.
type Executor struct {
	ID            int64          `gorm:"primaryKey;autoIncrement" json:"id"`
	AppName       string         `gorm:"size:64;not null"         json:"app_name"`
	Title         string         `gorm:"size:64"                  json:"title"`
	Address       string         `gorm:"size:128;not null"        json:"address"`
	RegisterType  RegisterType   `gorm:"not null;default:0"       json:"register_type"`
	Status        ExecutorStatus `gorm:"not null;default:1"       json:"status"`
	Weight        int            `gorm:"not null;default:1"       json:"weight"`
	HeartbeatTime *time.Time     `gorm:"column:heartbeat_time"    json:"heartbeat_time"`
	Version       string         `gorm:"size:32"                  json:"version"`
	CPU           float64        `gorm:"default:0"                json:"cpu"`
	Memory        float64        `gorm:"default:0"                json:"memory"`
	Tags          string         `gorm:"size:255"                 json:"tags"`
	CreateTime    time.Time      `gorm:"autoCreateTime"           json:"create_time"`
	UpdateTime    time.Time      `gorm:"autoUpdateTime"           json:"update_time"`
}

func (Executor) TableName() string { return "sys_executor" }

// IsOnline returns true if the executor is considered alive.
func (e *Executor) IsOnline() bool { return e.Status == ExecutorOnline }
