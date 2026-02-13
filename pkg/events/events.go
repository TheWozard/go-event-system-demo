package events

import (
	"strings"
	"time"
)

const (
	StatusActive  Status = "active"
	StatusDeleted Status = "deleted"
)

type Status string

type Event[T any] struct {
	Context Context
	Data    T
}

type Context struct {
	ID        string
	Status    Status
	Source    string
	Timestamp time.Time
}

func (c Context) Split(id string) Context {
	return Context{
		ID:        id,
		Status:    c.Status,
		Source:    c.Source,
		Timestamp: c.Timestamp,
	}
}

func (c Context) Append(source string) Context {
	return Context{
		ID:        c.ID,
		Status:    c.Status,
		Source:    c.Source + ":" + source,
		Timestamp: c.Timestamp,
	}
}

func (c Context) Sources() []string {
	return strings.Split(c.Source, ":")
}

type Handler[T any] func(Event[T]) error
type AsyncHandler[T any] func(Event[T])
