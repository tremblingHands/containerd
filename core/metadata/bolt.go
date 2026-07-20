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

package metadata

import (
	"context"
	"fmt"

	bolt "go.etcd.io/bbolt"
	errbolt "go.etcd.io/bbolt/errors"

	"github.com/containerd/containerd/v2/core/metadata/boltutil"
	"github.com/containerd/containerd/v2/pkg/tracing"
)

// Transactor is the database interface for running transactions
type Transactor interface {
	View(fn func(*bolt.Tx) error) error
	Update(fn func(*bolt.Tx) error) error
}

// view gets a bolt db transaction either from the context
// or starts a new one with the provided bolt database.
func view(ctx context.Context, db Transactor, fn func(*bolt.Tx) error) error {
	tx, ok := boltutil.Transaction(ctx)
	if !ok {
		return db.View(fn)
	}
	return fn(tx)
}

// update gets a writable bolt db transaction either from the context
// or starts a new one with the provided bolt database.
func update(ctx context.Context, db Transactor, fn func(*bolt.Tx) error) error {
	tx, ok := boltutil.Transaction(ctx)
	if !ok {
		return db.Update(fn)
	} else if !tx.Writable() {
		return fmt.Errorf("unable to use transaction from context: %w", errbolt.ErrTxNotWritable)
	}
	return fn(tx)
}

// updateTraced runs a writable bolt transaction with TRACE sub-spans:
//
//	{prefix}.tx          — whole call
//	  {prefix}.tx.wait   — until bolt write lock is acquired (callback entered)
//	  {prefix}.tx.exec   — user callback
//	  {prefix}.tx.commit — after callback returns until db.Update returns
//
// Under concurrency, .wait usually dominates.
func updateTraced(ctx context.Context, db Transactor, prefix string, fn func(context.Context, *bolt.Tx) error) error {
	if tx, ok := boltutil.Transaction(ctx); ok {
		if !tx.Writable() {
			return fmt.Errorf("unable to use transaction from context: %w", errbolt.ErrTxNotWritable)
		}
		return fn(ctx, tx)
	}

	tctx, ts := tracing.StartSpan(ctx, tracing.Name(prefix, "tx"))
	defer ts.End()

	_, waitSpan := tracing.StartSpan(tctx, tracing.Name(prefix, "tx", "wait"))
	waitEnded := false
	var commitSpan *tracing.Span

	err := db.Update(func(tx *bolt.Tx) error {
		if !waitEnded {
			waitSpan.End()
			waitEnded = true
		}

		ectx, es := tracing.StartSpan(tctx, tracing.Name(prefix, "tx", "exec"))
		ferr := fn(ectx, tx)
		es.End()

		_, commitSpan = tracing.StartSpan(tctx, tracing.Name(prefix, "tx", "commit"))
		return ferr
	})

	if !waitEnded {
		waitSpan.End()
	}
	if commitSpan != nil {
		commitSpan.End()
	}
	return err
}
