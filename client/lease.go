/*
   Copyright The containerd Authors.
   ...
*/

package client

import (
	"context"
	"time"

	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/containerd/v2/pkg/tracing"
)

// WithLease attaches a lease on the context
func (c *Client) WithLease(ctx context.Context, opts ...leases.Opt) (context.Context, func(context.Context) error, error) {
	// ★ 1. 启动 span (继承父 span, 如 client.NewContainer)
	ctx, span := tracing.StartSpan(ctx, tracing.Name("client", "WithLease"))
	defer span.End()

	nop := func(context.Context) error { return nil }

	// ★ 2. 已有 lease, 复用
	if existing, ok := leases.FromContext(ctx); ok {
		span.SetAttributes(
			tracing.Attribute("lease.action", "reuse"),
			tracing.Attribute("lease.id", existing),
		)
		return ctx, nop, nil
	}

	ls := c.LeasesService()

	// ★ 3. 默认 opts
	if len(opts) == 0 {
		opts = []leases.Opt{
			leases.WithRandomID(),
			leases.WithExpiration(24 * time.Hour),
		}
		span.SetAttributes(
			tracing.Attribute("lease.expiration", "24h"),
		)
	}

	// ★ 4. 创建 lease
	l, err := ls.Create(ctx, opts...)
	if err != nil {
		span.SetStatus(err)  // ★★★ 记录错误
		span.SetAttributes(tracing.Attribute("lease.action", "create"))
		return ctx, nop, err
	}

	span.SetAttributes(
		tracing.Attribute("lease.action", "create"),
		tracing.Attribute("lease.id", l.ID),
	)

	ctx = leases.WithLease(ctx, l.ID)
	return ctx, func(ctx context.Context) error {
		// ★ 5. 删除是 deferred 操作, 用 Event 而非子 span
		span.AddEvent("lease.delete",
			tracing.Attribute("lease.id", l.ID),
		)
		return ls.Delete(ctx, l)
	}, nil
}
