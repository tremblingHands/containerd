/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
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
	// parentCtx is returned to the caller so later spans (NewContainer.opt,
	// snapshotter.Prepare, …) nest under the caller's span (e.g.
	// client.NewContainer), not under this short-lived WithLease span.
	// Previously returning the span ctx made every subsequent StartSpan a
	// child of WithLease even after span.End(), which broke --summary-tree
	// hierarchy (children longer than WithLease itself).
	parentCtx := ctx
	spanCtx, span := tracing.StartSpan(parentCtx, tracing.Name("client", "WithLease"))
	defer span.End()

	nop := func(context.Context) error { return nil }

	// Already have a lease: reuse it on the parent context.
	if existing, ok := leases.FromContext(parentCtx); ok {
		span.SetAttributes(
			tracing.Attribute("lease.action", "reuse"),
			tracing.Attribute("lease.id", existing),
		)
		return parentCtx, nop, nil
	}

	ls := c.LeasesService()

	if len(opts) == 0 {
		opts = []leases.Opt{
			leases.WithRandomID(),
			leases.WithExpiration(24 * time.Hour),
		}
		span.SetAttributes(
			tracing.Attribute("lease.expiration", "24h"),
		)
	}

	// Create under the WithLease span so lease RPC time is attributed here.
	l, err := ls.Create(spanCtx, opts...)
	if err != nil {
		span.SetStatus(err)
		span.SetAttributes(tracing.Attribute("lease.action", "create"))
		return parentCtx, nop, err
	}

	span.SetAttributes(
		tracing.Attribute("lease.action", "create"),
		tracing.Attribute("lease.id", l.ID),
	)

	out := leases.WithLease(parentCtx, l.ID)
	return out, func(ctx context.Context) error {
		// Delete runs after the WithLease span has ended (caller defers done);
		// use an event on the already-ended span for correlation only.
		span.AddEvent("lease.delete",
			tracing.Attribute("lease.id", l.ID),
		)
		return ls.Delete(ctx, l)
	}, nil
}
