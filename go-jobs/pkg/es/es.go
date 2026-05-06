// Package es provides an ElasticSearch client for go-jobs log storage.
// Full search implementation is described in VERSIONS.md (v3.0 section).
package es

import (
	"fmt"

	"github.com/elastic/go-elasticsearch/v8"
)

// Options configures the ES client.
type Options struct {
	Addresses []string
	Username  string
	Password  string
	Index     string
}

// Option is a functional option.
type Option func(*Options)

// WithAddresses sets the ES node addresses.
func WithAddresses(addrs []string) Option { return func(o *Options) { o.Addresses = addrs } }

// WithCredentials sets the username and password.
func WithCredentials(u, p string) Option { return func(o *Options) { o.Username = u; o.Password = p } }

// WithIndex sets the default index name.
func WithIndex(idx string) Option { return func(o *Options) { o.Index = idx } }

// Client wraps the ES client.
type Client struct {
	*elasticsearch.Client
	Index string
}

// New creates a new ES Client.
func New(opts ...Option) (*Client, error) {
	o := &Options{Index: "go-jobs-logs"}
	for _, opt := range opts {
		opt(o)
	}
	cli, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: o.Addresses,
		Username:  o.Username,
		Password:  o.Password,
	})
	if err != nil {
		return nil, fmt.Errorf("es: new client: %w", err)
	}
	return &Client{Client: cli, Index: o.Index}, nil
}
