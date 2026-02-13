package handler

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"github.com/TheWozard/go-event-system-demo/pkg/events"
)

type SourceRouter[T any] map[string]events.Handler[T]

func (p SourceRouter[T]) Handler(event events.Event[T]) error {
	for _, source := range event.Context.Sources() {
		if handler, ok := p[source]; ok {
			return handler(event)
		}
	}
	return fmt.Errorf("unknown route for source %s", event.Context.Source)
}

type TypeBasedRouter map[reflect.Type]events.Handler[any]

func RegisterType[T any](t TypeBasedRouter, h events.Handler[any]) {
	t[reflect.TypeFor[T]()] = h
}

func (r TypeBasedRouter) Handler(event events.Event[any]) error {
	if handler, ok := r[reflect.TypeOf(event.Data)]; ok {
		return handler(event)
	}
	return fmt.Errorf("no handler for event data types")
}

func (r TypeBasedRouter) HandleAll(events []events.Event[any]) error {
	for _, event := range events {
		if err := r.Handler(event); err != nil {
			return err
		}
	}
	return nil
}

func TypedDataHandler[D any](wrapped func(events.Event[D]) error) events.Handler[any] {
	return func(e events.Event[any]) error {
		event := events.Event[D]{
			Context: e.Context,
		}
		switch typed := e.Data.(type) {
		case D:
			event.Data = typed
		case []byte:
			if err := json.Unmarshal(typed, &event.Data); err != nil {
				return err
			}
		case string:
			if err := json.Unmarshal([]byte(typed), &event.Data); err != nil {
				return err
			}
		case io.Reader:
			if err := json.NewDecoder(typed).Decode(&event.Data); err != nil {
				return err
			}
		default:
			return fmt.Errorf("failed to process event of type %T", e.Data)
		}
		return wrapped(event)
	}
}
