package file

import (
	"encoding/json"
	"sync"

	"github.com/TheWozard/go-event-system-demo/pkg/events"
)

func NewQueue(name, path string) *Queue {
	return &Queue{name: name, path: path}
}

type Queue struct {
	mu      sync.Mutex
	name    string
	path    string
	Handler events.AsyncHandler[any]
}

func (q *Queue) Handle(event events.Event[any]) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	file, _ := openOrCreate(q.path)
	defer file.Close()
	_, _ = file.Seek(0, 2)
	if err := json.NewEncoder(file).Encode(event); err != nil {
		return err
	}
	if q.Handler == nil {
		return nil
	}
	go func() {
		event.Context = event.Context.Append(q.name)
		q.Handler(rawEvent(event))
	}()
	return nil
}
