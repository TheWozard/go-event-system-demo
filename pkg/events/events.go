package events

import "time"

const (
	StatusActive  Status = "active"
	StatusDeleted Status = "deleted"

	ResultUpdate Result = "update"
	ResultDelete Result = "delete"
	ResultFilter Result = "filter"
	ResultDrop   Result = "drop"
	ResultError  Result = "error"
)

type Event struct {
	ID        string
	Status    Status
	Source    string
	Timestamp time.Time
	Data      map[string]interface{}
}

type Result string

type Handler func(Event) (Result, error)
type AsyncHandler func(Event)

type Status string

type Destination interface {
	Write(interface{}) (Result, error)
}
