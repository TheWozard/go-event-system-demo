package file

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/TheWozard/go-event-system-demo/pkg/events"
)

func openOrCreate(path string) (*os.File, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
}

func rawEvent(event events.Event[any]) events.Event[any] {
	switch event.Data.(type) {
	case []byte:
		return event
	default:
		raw, _ := json.Marshal(event.Data)
		return events.Event[any]{
			Context: event.Context,
			Data:    raw,
		}
	}
}
