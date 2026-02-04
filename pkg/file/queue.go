package file

import (
	"encoding/json"
	"sync"

	"github.com/TheWozard/go-event-system-demo/pkg/events"
)

func NewQueue(path string) *Queue {
	return &Queue{path: path}
}

type Queue struct {
	mu      sync.Mutex
	path    string
	Handler events.Handler
}

func (q *Queue) Handle(event events.Event) (events.Result, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	file, _ := openOrCreate(q.path)
	defer file.Close()
	_, _ = file.Seek(0, 2)
	if err := json.NewEncoder(file).Encode(event); err != nil {
		return events.ResultError, err
	}
	if q.Handler == nil {
		return events.ResultFilter, nil
	}
	return q.Handler(event)
}
