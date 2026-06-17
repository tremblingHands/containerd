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

package cni

import (
	"context"
	"fmt"
	"os"
	"time"

	cnilibrary "github.com/containernetworking/cni/libcni"
	types100 "github.com/containernetworking/cni/pkg/types/100"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type Network struct {
	cni    cnilibrary.CNI
	config *cnilibrary.NetworkConfigList
	ifName string
}

func (n *Network) Attach(ctx context.Context, ns *Namespace) (*types100.Result, error) {
	tracer := otel.Tracer("")
	parentSpan := trace.SpanFromContext(ctx)
	parentSpanID := trace.SpanID{}
	if parentSpan != nil {
		parentSpanID = parentSpan.SpanContext().SpanID()
	}
	_, span := tracer.Start(ctx, "cni.network.attach")
	start := time.Now()
	fmt.Fprintf(os.Stderr, "[TRACE] start name=%q trace=%s span=%s parent=%s\n",
		"cni.network.attach",
		span.SpanContext().TraceID(),
		span.SpanContext().SpanID(),
		parentSpanID)
	defer func() {
		span.End()
		fmt.Fprintf(os.Stderr, "[TRACE] end name=%q trace=%s span=%s dur=%s\n",
			"cni.network.attach",
			span.SpanContext().TraceID(),
			span.SpanContext().SpanID(),
			time.Since(start))
	}()

	r, err := n.cni.AddNetworkList(ctx, n.config, ns.config(n.ifName))
	if err != nil {
		return nil, err
	}
	return types100.NewResultFromResult(r)
}

func (n *Network) Remove(ctx context.Context, ns *Namespace) error {
	return n.cni.DelNetworkList(ctx, n.config, ns.config(n.ifName))
}

func (n *Network) Check(ctx context.Context, ns *Namespace) error {
	return n.cni.CheckNetworkList(ctx, n.config, ns.config(n.ifName))
}

type Namespace struct {
	id             string
	path           string
	capabilityArgs map[string]interface{}
	args           map[string]string
}

func newNamespace(id, path string, opts ...NamespaceOpts) (*Namespace, error) {
	ns := &Namespace{
		id:             id,
		path:           path,
		capabilityArgs: make(map[string]interface{}),
		args:           make(map[string]string),
	}
	for _, o := range opts {
		if err := o(ns); err != nil {
			return nil, err
		}
	}
	return ns, nil
}

func (ns *Namespace) config(ifName string) *cnilibrary.RuntimeConf {
	c := &cnilibrary.RuntimeConf{
		ContainerID: ns.id,
		NetNS:       ns.path,
		IfName:      ifName,
	}
	for k, v := range ns.args {
		c.Args = append(c.Args, [2]string{k, v})
	}
	c.CapabilityArgs = ns.capabilityArgs
	return c
}
