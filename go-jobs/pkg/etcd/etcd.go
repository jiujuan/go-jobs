// Package etcd wraps the etcd v3 client for use in go-jobs.
// Full leader-election implementation is described in VERSIONS.md (v3.0 section).
package etcd

import (
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Options configures the etcd client.
type Options struct {
	Endpoints   []string
	DialTimeout time.Duration
	Username    string
	Password    string
}

// Option is a functional option for Options.
type Option func(*Options)

// WithEndpoints sets the etcd endpoints.
func WithEndpoints(eps []string) Option { return func(o *Options) { o.Endpoints = eps } }

// WithDialTimeout sets the dial timeout.
func WithDialTimeout(d time.Duration) Option { return func(o *Options) { o.DialTimeout = d } }

// WithCredentials sets the username and password.
func WithCredentials(u, p string) Option {
	return func(o *Options) { o.Username = u; o.Password = p }
}

func defaultOptions() *Options {
	return &Options{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	}
}

// NewClient creates an etcd v3 client.
func NewClient(opts ...Option) (*clientv3.Client, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   o.Endpoints,
		DialTimeout: o.DialTimeout,
		Username:    o.Username,
		Password:    o.Password,
	})
	if err != nil {
		return nil, fmt.Errorf("etcd: new client: %w", err)
	}
	return cli, nil
}
