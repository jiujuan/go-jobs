package model

import "time"

// UserRole defines admin vs regular user access level.
type UserRole int8

const (
	RoleAdmin  UserRole = 1
	RoleNormal UserRole = 2
)

// UserStatus reflects whether the account is active.
type UserStatus int8

const (
	UserDisabled UserStatus = 0
	UserEnabled  UserStatus = 1
)

// SysUser maps to sys_user.
type SysUser struct {
	ID         int64      `gorm:"primaryKey;autoIncrement"     json:"id"`
	Username   string     `gorm:"size:32;not null;uniqueIndex" json:"username"`
	Password   string     `gorm:"size:128;not null"            json:"-"` // never serialise password hash
	Nickname   string     `gorm:"size:64"                      json:"nickname"`
	Email      string     `gorm:"size:128"                     json:"email"`
	Role       UserRole   `gorm:"not null;default:2"           json:"role"`
	Status     UserStatus `gorm:"not null;default:1"           json:"status"`
	LastLogin  *time.Time `gorm:"column:last_login"            json:"last_login"`
	CreateTime time.Time  `gorm:"autoCreateTime"               json:"create_time"`
	UpdateTime time.Time  `gorm:"autoUpdateTime"               json:"update_time"`
}

func (SysUser) TableName() string { return "sys_user" }

// IsAdmin returns true if the user has admin privileges.
func (u *SysUser) IsAdmin() bool { return u.Role == RoleAdmin }

// IsActive returns true if the user account is enabled.
func (u *SysUser) IsActive() bool { return u.Status == UserEnabled }
