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

package tracing

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// StartConfig defines configuration for a new span object.
type StartConfig struct {
	spanOpts []trace.SpanStartOption
	attrs    []attribute.KeyValue
}

type SpanOpt func(config *StartConfig)

// WithAttribute appends attributes to a new created span.
func WithAttribute(k string, v any) SpanOpt {
	return func(config *StartConfig) {
		attr := Attribute(k, v)
		config.spanOpts = append(config.spanOpts, trace.WithAttributes(attr))
		config.attrs = append(config.attrs, attr)
	}
}

// initTP guards lazy initialization of a default TracerProvider
// when the OTLP tracing plugin has not been loaded.
var initTP sync.Once

// ensureTracerProvider ensures a valid TracerProvider is set globally.
// When the OTLP tracing plugin is skipped (no endpoint configured), the
// default no-op TracerProvider generates zero trace/span IDs. This function
// installs an SDK TracerProvider that generates valid random IDs, so that
// [TRACE] log output and the logrus trace_id field are useful even without
// an exporter.
func ensureTracerProvider() {
	initTP.Do(func() {
		// If a proper TracerProvider is already configured (e.g. by the
		// OTLP tracing plugin), the span context will be valid — skip.
		tracer := otel.Tracer("")
		_, span := tracer.Start(context.Background(), "_ensure_tp")
		defer span.End()
		if span.SpanContext().IsValid() {
			return
		}
		// No valid provider — install a default SDK TracerProvider.
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{}, propagation.Baggage{},
		))
		otel.SetTracerProvider(sdktrace.NewTracerProvider())
	})
}

// UpdateHTTPClient updates the http client with the necessary otel transport
func UpdateHTTPClient(client *http.Client, name string) {
	client.Transport = otelhttp.NewTransport(
		client.Transport,
		otelhttp.WithSpanNameFormatter(func(operation string, r *http.Request) string {
			return name
		}),
	)
}

// StartSpan starts child span in a context.
func StartSpan(ctx context.Context, opName string, opts ...SpanOpt) (context.Context, *Span) {
	ensureTracerProvider()

	config := StartConfig{}
	for _, fn := range opts {
		fn(&config)
	}

	// Capture parent span ID before creating the child span.
	var parentSpanID trace.SpanID
	if parent := trace.SpanFromContext(ctx); parent != nil && parent.SpanContext().IsValid() {
		parentSpanID = parent.SpanContext().SpanID()
	}

	tracer := otel.Tracer("")
	if parentSpanID.IsValid() {
		tracer = trace.SpanFromContext(ctx).TracerProvider().Tracer("")
	}
	ctx, span := tracer.Start(ctx, opName, config.spanOpts...)

	// Format attributes for stderr output
	attrStr := formatAttrs(config.attrs)

	fmt.Fprintf(os.Stderr, "[TRACE] start name=%q trace=%s span=%s parent=%s%s\n",
		opName,
		span.SpanContext().TraceID(),
		span.SpanContext().SpanID(),
		parentSpanID,
		attrStr)

	return ctx, &Span{
		otelSpan: span,
		opName:   opName,
		start:    time.Now(),
		attrs:    config.attrs,
	}
}

// SpanFromContext returns the current Span from the context.
func SpanFromContext(ctx context.Context) *Span {
	return &Span{
		otelSpan: trace.SpanFromContext(ctx),
	}
}

// Span is wrapper around otel trace.Span.
// Span is the individual component of a trace. It represents a
// single named and timed operation of a workflow that is traced.
type Span struct {
	otelSpan trace.Span
	opName   string
	start    time.Time
	attrs    []attribute.KeyValue
}

// End completes the span.
func (s *Span) End() {
	s.otelSpan.End()
	if s.start.IsZero() {
		return
	}
	dur := time.Since(s.start)
	traceID := s.otelSpan.SpanContext().TraceID().String()
	spanID := s.otelSpan.SpanContext().SpanID().String()
	name := s.opName
	if name == "" {
		name = "<unknown>"
	}
	attrStr := formatAttrs(s.attrs)
	fmt.Fprintf(os.Stderr, "[TRACE] end name=%q trace=%s span=%s dur=%s%s\n",
		name, traceID, spanID, dur, attrStr)
}

// AddEvent adds an event with provided name and options.
func (s *Span) AddEvent(name string, attributes ...attribute.KeyValue) {
	s.otelSpan.AddEvent(name, trace.WithAttributes(attributes...))
}

// RecordError will record err as an exception span event for this span
func (s *Span) RecordError(err error, options ...trace.EventOption) {
	s.otelSpan.RecordError(err, options...)
}

// SetStatus sets the status of the current span.
// If an error is encountered, it records the error and sets span status to Error.
func (s *Span) SetStatus(err error) {
	if err != nil {
		s.otelSpan.RecordError(err)
		s.otelSpan.SetStatus(codes.Error, err.Error())
	} else {
		s.otelSpan.SetStatus(codes.Ok, "")
	}
}

// SetAttributes sets kv as attributes of the span.
func (s *Span) SetAttributes(kv ...attribute.KeyValue) {
	s.otelSpan.SetAttributes(kv...)
}

const spanDelimiter = "."

// Name sets the span name by joining a list of strings in dot separated format.
func Name(names ...string) string {
	return strings.Join(names, spanDelimiter)
}

// Attribute takes a key value pair and returns attribute.KeyValue type.
func Attribute(k string, v any) attribute.KeyValue {
	return keyValue(k, v)
}

// HTTPStatusCodeAttributes generates HTTP response status code attributes
// as specified by the current OpenTelemetry semantic conventions.
func HTTPStatusCodeAttributes(code int) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.Int("http.response.status_code", code),
		attribute.Int("http.status_code", code), // Deprecated: SemConv <= v1.21
	}
}

// formatAttrs formats a slice of attribute.KeyValue as a space-prefixed
// string of key=value pairs suitable for [TRACE] log output.
// Returns an empty string if attrs is empty.
func formatAttrs(attrs []attribute.KeyValue) string {
	if len(attrs) == 0 {
		return ""
	}
	var b strings.Builder
	for _, attr := range attrs {
		b.WriteString(" ")
		b.WriteString(string(attr.Key))
		b.WriteString("=")
		b.WriteString(attr.Value.AsString())
	}
	return b.String()
}
