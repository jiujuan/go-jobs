package dao

import (
	"context"
	"time"

	"github.com/jiujuan/go-jobs/internal/model"
	"gorm.io/gorm"
)

// UserDAO defines the persistence contract for system users.
type UserDAO interface {
	FindByUsername(ctx context.Context, username string) (*model.SysUser, error)
	FindByID(ctx context.Context, id int64) (*model.SysUser, error)
	Create(ctx context.Context, u *model.SysUser) error
	Update(ctx context.Context, u *model.SysUser) error
	UpdateLastLogin(ctx context.Context, id int64, t time.Time) error
}

type userDAO struct{ db *gorm.DB }

// NewUserDAO returns a GORM-backed UserDAO.
func NewUserDAO(db *gorm.DB) UserDAO { return &userDAO{db: db} }

func (d *userDAO) FindByUsername(ctx context.Context, username string) (*model.SysUser, error) {
	var u model.SysUser
	err := d.db.WithContext(ctx).Where("username = ?", username).First(&u).Error
	return &u, err
}

func (d *userDAO) FindByID(ctx context.Context, id int64) (*model.SysUser, error) {
	var u model.SysUser
	err := d.db.WithContext(ctx).First(&u, id).Error
	return &u, err
}

func (d *userDAO) Create(ctx context.Context, u *model.SysUser) error {
	return d.db.WithContext(ctx).Create(u).Error
}

func (d *userDAO) Update(ctx context.Context, u *model.SysUser) error {
	return d.db.WithContext(ctx).Save(u).Error
}

func (d *userDAO) UpdateLastLogin(ctx context.Context, id int64, t time.Time) error {
	return d.db.WithContext(ctx).Model(&model.SysUser{}).
		Where("id = ?", id).
		Update("last_login", t).Error
}
