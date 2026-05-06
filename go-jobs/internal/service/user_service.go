package service

import (
	"context"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"

	"github.com/jiujuan/go-jobs/internal/dao"
	"github.com/jiujuan/go-jobs/internal/model"
	"github.com/jiujuan/go-jobs/pkg/logger"
	"github.com/jiujuan/go-jobs/pkg/xerror"
)

// UserService handles authentication and user management.
type UserService struct {
	userDAO        dao.UserDAO
	jwtSecret      []byte
	jwtExpireDur   time.Duration
}

// NewUserService creates a UserService.
func NewUserService(userDAO dao.UserDAO, jwtSecret string, jwtExpireDur time.Duration) *UserService {
	return &UserService{
		userDAO:      userDAO,
		jwtSecret:    []byte(jwtSecret),
		jwtExpireDur: jwtExpireDur,
	}
}

// Claims is the JWT payload.
type Claims struct {
	UserID   int64            `json:"uid"`
	Username string           `json:"username"`
	Role     model.UserRole   `json:"role"`
	jwt.RegisteredClaims
}

// LoginRequest carries login credentials.
type LoginRequest struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

// LoginResponse is returned on successful login.
type LoginResponse struct {
	Token    string     `json:"token"`
	ExpireAt time.Time  `json:"expire_at"`
	User     *model.SysUser `json:"user"`
}

// Login validates credentials and returns a signed JWT.
func (s *UserService) Login(ctx context.Context, req *LoginRequest) (*LoginResponse, error) {
	user, err := s.userDAO.FindByUsername(ctx, req.Username)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, xerror.New(xerror.CodePasswordWrong)
		}
		return nil, xerror.Wrap(xerror.CodeInternalServer, err)
	}

	if !user.IsActive() {
		return nil, xerror.New(xerror.CodeUserDisabled)
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(req.Password)); err != nil {
		return nil, xerror.New(xerror.CodePasswordWrong)
	}

	expireAt := time.Now().Add(s.jwtExpireDur)
	claims := &Claims{
		UserID:   user.ID,
		Username: user.Username,
		Role:     user.Role,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expireAt),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Issuer:    "go-jobs",
		},
	}

	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.jwtSecret)
	if err != nil {
		return nil, xerror.Wrap(xerror.CodeInternalServer, err, "sign token failed")
	}

	_ = s.userDAO.UpdateLastLogin(ctx, user.ID, time.Now())

	logger.Info("user login", zap.String("username", user.Username))

	return &LoginResponse{
		Token:    token,
		ExpireAt: expireAt,
		User:     user,
	}, nil
}

// ParseToken validates a JWT and returns the embedded claims.
func (s *UserService) ParseToken(tokenStr string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenStr, &Claims{}, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return s.jwtSecret, nil
	})
	if err != nil {
		if jwt.IsExpirationError(err) {
			return nil, xerror.New(xerror.CodeTokenExpired)
		}
		return nil, xerror.New(xerror.CodeTokenInvalid)
	}
	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, xerror.New(xerror.CodeTokenInvalid)
	}
	return claims, nil
}

// GetUserByID fetches a user by primary key.
func (s *UserService) GetUserByID(ctx context.Context, id int64) (*model.SysUser, error) {
	u, err := s.userDAO.FindByID(ctx, id)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, xerror.New(xerror.CodeNotFound)
		}
		return nil, xerror.Wrap(xerror.CodeInternalServer, err)
	}
	return u, nil
}

// HashPassword returns a bcrypt hash of the given plaintext password.
func HashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", fmt.Errorf("hash password: %w", err)
	}
	return string(hash), nil
}
