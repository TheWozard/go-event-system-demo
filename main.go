package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/TheWozard/go-event-system-demo/pkg/events"
	"github.com/TheWozard/go-event-system-demo/pkg/file"
)

func main() {
	// Tables.
	rawActor := file.NewTable("./data/raw/actor.json")
	rawEpisodes := file.NewTable("./data/raw/episodes.json")
	rawMovie := file.NewTable("./data/raw/movie.json")
	rawSeries := file.NewTable("./data/raw/series.json")
	standardPerson := file.NewTable("./data/standard/person.json")
	standardVideo := file.NewTable("./data/standard/video.json")
	encodedData := file.NewTable("./data/encoded/data.json")

	// Queues.
	rawQueue := file.NewQueue("./data/raw.log")
	personQueue := file.NewQueue("./data/person.log")
	videoQueue := file.NewQueue("./data/video.log")
	encodeQueue := file.NewQueue("./data/encode.log")

	// Wiring tables to produce events to queues.
	rawActor.Handler = async(rawQueue.Handle)
	rawEpisodes.Handler = async(rawQueue.Handle)
	rawMovie.Handler = async(rawQueue.Handle)
	rawSeries.Handler = async(rawQueue.Handle)
	standardPerson.Handler = async(personQueue.Handle)
	standardVideo.Handler = async(videoQueue.Handle)

	// Wiring queues to process events.
	rawQueue.Handler = rawProcessor{
		episodeHandler: rawEpisodes.Handle,
		personHandler:  standardPerson.Handle,
		videoHandler:   standardVideo.Handle,
		encodeHandler:  encodeQueue.Handle,
	}.Handle
	encodeQueue.Handler = delay(1*time.Second, encodeData(encodedData.Handle))

	// Endpoints enable sending messages into the system.
	mux := http.NewServeMux()
	attachHandler(mux, "/rawActors", rawActor.Handle)
	attachHandler(mux, "/rawMovies", rawMovie.Handle)
	attachHandler(mux, "/rawSeries", rawSeries.Handle)

	// Start server.
	log.Println("Starting server on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}

// -- Handlers --

type rawProcessor struct {
	episodeHandler events.Handler
	personHandler  events.Handler
	videoHandler   events.Handler

	encodeHandler events.Handler
}

func (r rawProcessor) Handle(e events.Event) (events.Result, error) {
	if strings.Contains(e.Source, "actor") {
		return r.actor(e)
	} else if strings.Contains(e.Source, "episode") {
		return r.episodes(e)
	} else if strings.Contains(e.Source, "movie") {
		return r.movie(e)
	} else if strings.Contains(e.Source, "series") {
		return r.splitSeries(e)
	}
	return events.ResultDrop, nil
}

func (r rawProcessor) actor(e events.Event) (events.Result, error) {
	return r.personHandler(e)
}

func (r rawProcessor) episodes(e events.Event) (events.Result, error) {
	combined := fmt.Sprintf("%s_ep_%d", e.Data["seriesTitle"].(string), e.Data["episodeNumber"].(int))
	r.encode(combined, e)
	return r.movie(e)
}

func (r rawProcessor) movie(e events.Event) (events.Result, error) {
	r.encode(e.Data["title"].(string), e)
	return r.videoHandler(e)
}

func (r rawProcessor) encode(file string, e events.Event) {
	file = strings.ReplaceAll(strings.ToLower(file), " ", "_")
	async(r.encodeHandler)(events.Event{
		ID:        e.ID,
		Status:    e.Status,
		Source:    "video",
		Timestamp: time.Now().UTC(),
		Data: map[string]interface{}{
			"location": fmt.Sprintf("file://%s.mkv", file),
		},
	})
}

func (r rawProcessor) splitSeries(e events.Event) (events.Result, error) {
	episodes, ok := e.Data["episodes"].([]interface{})
	if !ok {
		return events.ResultError, fmt.Errorf("invalid episodes data")
	}
	for i, ep := range episodes {
		epData, ok := ep.(map[string]interface{})
		if !ok {
			continue
		}
		epData["seriesID"] = e.ID
		epData["seriesTitle"] = e.Data["title"]
		epData["episodeNumber"] = i + 1
		async(r.episodeHandler)(events.Event{
			ID:        fmt.Sprintf("%s_ep%d", e.ID, i+1),
			Status:    e.Status,
			Source:    "splitSeries",
			Timestamp: time.Now().UTC(),
			Data:      epData,
		})
	}
	return events.ResultUpdate, nil
}

func encodeData(store events.Handler) events.Handler {
	return func(e events.Event) (events.Result, error) {
		hash := sha256.Sum256([]byte(e.Data["location"].(string)))
		e.Data["encodedHash"] = fmt.Sprintf("%x", hash)
		e.Data["location"] = strings.ReplaceAll(e.Data["location"].(string), ".mkv", "_encoded.mkv")
		return store(e)
	}
}

// -- Utility Functions --

func async(h events.Handler) events.AsyncHandler {
	return func(e events.Event) {
		go func() {
			result, err := h(e)
			writeEvent(os.Stderr, e, result, err)
		}()
	}
}

func delay(d time.Duration, h events.Handler) events.Handler {
	return func(e events.Event) (events.Result, error) {
		time.Sleep(d)
		return h(e)
	}
}

func attachHandler(mux *http.ServeMux, path string, handler events.Handler) {
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		status := r.URL.Query().Get("status")
		data := map[string]interface{}{}
		_ = json.NewDecoder(r.Body).Decode(&data)
		event := events.Event{
			ID:        id,
			Status:    events.Status(status),
			Source:    path,
			Timestamp: time.Now().UTC(),
			Data:      data,
		}
		result, err := handler(event)
		writeEvent(w, event, result, err)
	})
}

func writeEvent(w io.Writer, event events.Event, result events.Result, err error) {
	timestamp := event.Timestamp.Format("03:04:05.000")
	if err != nil {
		fmt.Fprintf(w, "%s %-30s | %-15s | %s: %s\n", timestamp, event.Source, event.ID, result, err.Error())
	} else {
		fmt.Fprintf(w, "%s %-30s | %-15s | %s\n", timestamp, event.Source, event.ID, result)
	}
}
