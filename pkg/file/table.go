package file

import (
	"crypto/sha256"
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/TheWozard/go-event-system-demo/pkg/events"
)

func NewTable(name, path string) *Table {
	return &Table{name: name, path: path}
}

type Table struct {
	mu      sync.Mutex
	name    string
	path    string
	Handler events.AsyncHandler[any]
}

func (t *Table) Handle(event events.Event[any]) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	var fileData table
	file, _ := openOrCreate(t.path)
	defer file.Close()

	_ = json.NewDecoder(file).Decode(&fileData)
	records := fileData.ToMap()
	if event.Context.Status == events.StatusDeleted {
		existing, ok := records[event.Context.ID]
		if ok && event.Context.Timestamp.Before(existing.Time()) {
			return nil
		}
		records[event.Context.ID] = tableRecord{
			ID:        event.Context.ID,
			Timestamp: event.Context.Timestamp.UTC().Format(time.RFC3339Nano),
			Data:      existing.Data,
			Deleted:   true,
		}
	} else {
		newRecord := tableRecord{
			ID:        event.Context.ID,
			Timestamp: event.Context.Timestamp.UTC().Format(time.RFC3339Nano),
			Data:      event.Data,
		}
		if existing, ok := records[event.Context.ID]; ok {
			if event.Context.Timestamp.Before(existing.Time()) {
				return nil
			}
			if !existing.Deleted && existing.Hash() == newRecord.Hash() {
				return nil
			}
		}
		records[event.Context.ID] = newRecord
	}
	fileData.FromMap(records)

	_ = file.Truncate(0)
	_, _ = file.Seek(0, 0)
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(&fileData)

	if t.Handler != nil {
		// Create a new event for the change to this table.
		t.Handler(rawEvent(events.Event[any]{
			Context: events.Context{
				ID:        event.Context.ID,
				Status:    event.Context.Status,
				Source:    t.name,
				Timestamp: time.Now().UTC(),
			},
			Data: event.Data,
		}))
	}

	return nil
}

func (t *Table) Sync(prefix string, timestamp time.Time) error {
	timestamp = timestamp.Truncate(time.Nanosecond)
	t.mu.Lock()
	defer t.mu.Unlock()

	var fileData table
	file, _ := openOrCreate(t.path)
	defer file.Close()

	_ = json.NewDecoder(file).Decode(&fileData)
	records := fileData.ToMap()

	var removed []tableRecord
	for id, record := range records {
		if strings.HasPrefix(id, prefix) && !record.Deleted && record.Time().Before(timestamp) {
			record.Deleted = true
			record.Timestamp = timestamp.UTC().Format(time.RFC3339Nano)
			records[id] = record
			removed = append(removed, record)
		}
	}

	if len(removed) == 0 {
		return nil
	}

	fileData.FromMap(records)
	_ = file.Truncate(0)
	_, _ = file.Seek(0, 0)
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(&fileData)

	if t.Handler != nil {
		for _, record := range removed {
			t.Handler(rawEvent(events.Event[any]{
				Context: events.Context{
					ID:        record.ID,
					Status:    events.StatusDeleted,
					Source:    t.name,
					Timestamp: time.Now().UTC(),
				},
				Data: record.Data,
			}))
		}
	}

	return nil
}

type table struct {
	Records []tableRecord `json:"records"`
}

func (t *table) ToMap() map[string]tableRecord {
	m := make(map[string]tableRecord, len(t.Records))
	for _, r := range t.Records {
		m[r.ID] = r
	}
	return m
}

func (t *table) FromMap(m map[string]tableRecord) {
	t.Records = make([]tableRecord, 0, len(m))
	for _, r := range m {
		t.Records = append(t.Records, r)
	}
	slices.SortFunc(t.Records, func(a, b tableRecord) int {
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
}

type tableRecord struct {
	ID        string `json:"id"`
	Timestamp string `json:"timestamp"`
	Data      any    `json:"data"`
	Deleted   bool   `json:"deleted,omitempty"`
}

func (r tableRecord) Time() time.Time {
	t, _ := time.Parse(time.RFC3339Nano, r.Timestamp)
	return t
}

func (r tableRecord) Hash() [32]byte {
	data, _ := json.Marshal(r.Data)
	return sha256.Sum256(data)
}
