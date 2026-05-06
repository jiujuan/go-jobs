// Package etcd provides etcd client and leader-election for go-jobs v3.
// Multiple scheduler nodes compete for leadership; only the Leader runs the scheduling loop.
package etcd

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"

	"github.com/jiujuan/go-jobs/pkg/logger"
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

// ─── Leader Election ──────────────────────────────────────────────────────────

// LeaderElection 基于 etcd 实现分布式选主。
// 多个 scheduler 节点竞争同一 key，只有 Leader 运行调度循环。
type LeaderElection struct {
	client    *clientv3.Client
	session   *concurrency.Session
	election  *concurrency.Election
	nodeID    string
	isLeader  bool
	onElected func() // 成为 Leader 时调用（启动 scheduler）
	onRevoked func() // 失去 Leader 时调用（停止 scheduler）
	stopCh    chan struct{}
}

// NewLeaderElection 为给定 prefix 创建 LeaderElection。
// onElected 和 onRevoked 回调在对应事件时被调用（非阻塞）。
func NewLeaderElection(
	cli *clientv3.Client,
	prefix, nodeID string,
	onElected, onRevoked func(),
) (*LeaderElection, error) {
	sess, err := concurrency.NewSession(cli, concurrency.WithTTL(15))
	if err != nil {
		return nil, fmt.Errorf("etcd: new session: %w", err)
	}
	return &LeaderElection{
		client:    cli,
		session:   sess,
		election:  concurrency.NewElection(sess, prefix),
		nodeID:    nodeID,
		onElected: onElected,
		onRevoked: onRevoked,
		stopCh:    make(chan struct{}),
	}, nil
}

// Run 启动选主循环，阻塞直到 Stop() 被调用。应在独立 goroutine 中运行。
func (le *LeaderElection) Run(ctx context.Context) {
	for {
		select {
		case <-le.stopCh:
			logger.Info("etcd: leader election stopped", zap.String("nodeID", le.nodeID))
			return
		default:
		}

		logger.Info("etcd: campaigning for leader", zap.String("nodeID", le.nodeID))
		if err := le.election.Campaign(ctx, le.nodeID); err != nil {
			logger.Warn("etcd: campaign failed, retrying in 3s", zap.Error(err))
			select {
			case <-le.stopCh:
				return
			case <-time.After(3 * time.Second):
			}
			continue
		}

		le.isLeader = true
		logger.Info("etcd: elected as leader", zap.String("nodeID", le.nodeID))
		if le.onElected != nil {
			go le.onElected()
		}

		// 观察 Leader 变化，直到自己不再是 Leader。
		watchCh := le.election.Observe(ctx)
		for resp := range watchCh {
			if len(resp.Kvs) > 0 && string(resp.Kvs[0].Value) != le.nodeID {
				break
			}
		}

		le.isLeader = false
		logger.Warn("etcd: lost leadership", zap.String("nodeID", le.nodeID))
		if le.onRevoked != nil {
			go le.onRevoked()
		}
	}
}

// IsLeader 返回当前节点是否是 Leader。
func (le *LeaderElection) IsLeader() bool { return le.isLeader }

// Stop 停止选主循环并释放 session。
func (le *LeaderElection) Stop() {
	close(le.stopCh)
	if le.isLeader {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = le.election.Resign(ctx)
	}
	_ = le.session.Close()
}
