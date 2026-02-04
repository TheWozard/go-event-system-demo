package file

import (
	"crypto/sha256"
	"encoding/json"
	"slices"
	"sync"
	"time"

	"github.com/TheWozard/go-event-system-demo/pkg/events"
)

func NewTable(path string) *Table {
	return &Table{path: path}
}

type Table struct {
	mu      sync.Mutex
	path    string
	Handler events.AsyncHandler
}

func (t *Table) Handle(event events.Event) (events.Result, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var fileData table
	file, _ := openOrCreate(t.path)
	defer file.Close()

	_ = json.NewDecoder(file).Decode(&fileData)
	records := fileData.ToMap()
	var result events.Result
	if event.Status == events.StatusDeleted {
		existing, ok := records[event.ID]
		if !ok || event.Timestamp.Before(existing.Timestamp) {
			return events.ResultFilter, nil
		}
		delete(records, event.ID)
		result = events.ResultDelete
	} else {
		newRecord := tableRecord{
			ID:        event.ID,
			Timestamp: event.Timestamp,
			Data:      event.Data,
		}
		if existing, ok := records[event.ID]; ok {
			if event.Timestamp.Before(existing.Timestamp) {
				return events.ResultFilter, nil
			}
			if existing.Hash() == newRecord.Hash() {
				return events.ResultFilter, nil
			}
		}
		records[event.ID] = newRecord
		result = events.ResultUpdate
	}
	fileData.FromMap(records)

	_ = file.Truncate(0)
	_, _ = file.Seek(0, 0)
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(&fileData)

	if t.Handler != nil {
		// Create a new event for the change to this table.
		t.Handler(events.Event{
			ID:        event.ID,
			Status:    event.Status,
			Source:    t.path,
			Timestamp: time.Now().UTC(),
			Data:      event.Data,
		})
	}

	return result, nil
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
	ID        string                 `json:"id"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
}

func (r tableRecord) Hash() [32]byte {
	data, _ := json.Marshal(r.Data)
	return sha256.Sum256(data)
}
