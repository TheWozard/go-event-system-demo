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
	"github.com/TheWozard/go-event-system-demo/pkg/handler"
)

func main() {
	// Tables.
	rawActor := file.NewTable("rawActor", "./data/raw/actor.json")
	rawEpisodes := file.NewTable("rawEpisode", "./data/raw/episodes.json")
	rawMovie := file.NewTable("rawMovie", "./data/raw/movie.json")
	rawSeries := file.NewTable("rawSeries", "./data/raw/series.json")
	standardPerson := file.NewTable("standardPerson", "./data/standard/person.json")
	standardVideo := file.NewTable("standardVideo", "./data/standard/video.json")
	encodedData := file.NewTable("encodedData", "./data/encoded/data.json")

	// Queues.
	rawQueue := file.NewQueue("raw_q", "./data/raw.log")
	personQueue := file.NewQueue("person_q", "./data/person.log")
	videoQueue := file.NewQueue("video_q", "./data/video.log")
	encodeQueue := file.NewQueue("encode_q", "./data/encode.log")

	// Wiring tables to produce events to queues.
	rawActor.Handler = async(rawQueue.Handle)
	rawEpisodes.Handler = async(rawQueue.Handle)
	rawMovie.Handler = async(rawQueue.Handle)
	rawSeries.Handler = async(rawQueue.Handle)
	standardPerson.Handler = async(personQueue.Handle)
	standardVideo.Handler = async(videoQueue.Handle)

	rawDest := rawDestination{TypeBasedRouter: handler.TypeBasedRouter{}}
	handler.RegisterType[Person](rawDest.TypeBasedRouter, standardPerson.Handle)
	handler.RegisterType[Video](rawDest.TypeBasedRouter, standardVideo.Handle)
	handler.RegisterType[Encode](rawDest.TypeBasedRouter, encodeQueue.Handle)
	handler.RegisterType[RawEpisode](rawDest.TypeBasedRouter, rawEpisodes.Handle)
	rawDest.EpisodesTable = rawEpisodes
	rawQueue.Handler = async(handler.SourceRouter[any]{
		"rawActor":   handler.TypedDataHandler(rawDest.actor),
		"rawEpisode": handler.TypedDataHandler(rawDest.episodes),
		"rawMovie":   handler.TypedDataHandler(rawDest.movie),
		"rawSeries":  handler.TypedDataHandler(rawDest.splitSeries),
	}.Handler)

	encodeDest := encodeDestination{TypeBasedRouter: handler.TypeBasedRouter{}}
	handler.RegisterType[EncodeResult](encodeDest.TypeBasedRouter, encodedData.Handle)
	encodeQueue.Handler = async(delay(1*time.Second, handler.TypedDataHandler(encodeDest.Encode)))

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

type rawDestination struct {
	handler.TypeBasedRouter
	EpisodesTable *file.Table
}

type Person struct {
	Name string `json:"name"`
}

type Encode struct {
	Title    string `json:"title"`
	Location string `json:"location"`
}

type Video struct {
	Title string `json:"title"`
}

type RawEpisode map[string]any

func (d rawDestination) actor(event events.Event[struct {
	Name string `json:"name"`
}]) error {
	return d.Handler(events.Event[any]{
		Context: event.Context,
		Data: Person{
			Name: event.Data.Name,
		},
	})
}

func (d rawDestination) episodes(event events.Event[struct {
	SeriesTitle   string `json:"seriesTitle"`
	EpisodeNumber int    `json:"episodeNumber"`
}]) error {
	title := fmt.Sprintf("%s %d", event.Data.SeriesTitle, event.Data.EpisodeNumber)
	return d.encode(event.Context, Video{
		Title: title,
	})
}

func (d rawDestination) movie(event events.Event[struct {
	Title string `json:"title"`
}]) error {
	return d.encode(event.Context, Video{
		Title: event.Data.Title,
	})
}

func (d rawDestination) encode(context events.Context, video Video) error {
	file := strings.ReplaceAll(strings.ToLower(video.Title), " ", "_")
	return d.HandleAll([]events.Event[any]{
		{
			Context: context,
			Data:    video,
		},
		{
			Context: context,
			Data: Encode{
				Title:    video.Title,
				Location: fmt.Sprintf("file://%s.mkv", file),
			},
		},
	})
}

func (d rawDestination) splitSeries(event events.Event[struct {
	Title    string `json:"title"`
	Episodes []struct {
		Title string `json:"title"`
	} `json:"episodes"`
}]) error {
	for i, ep := range event.Data.Episodes {
		if err := d.Handler(events.Event[any]{
			Context: event.Context.Split(fmt.Sprintf("%s_ep%d", event.Context.ID, i+1)),
			Data: RawEpisode{
				"seriesTitle":  event.Data.Title,
				"episodeTitle": ep.Title,
				"seriesNumber": i + 1,
			},
		}); err != nil {
			return err
		}
	}
	return d.EpisodesTable.Sync(event.Context.ID, event.Context.Timestamp)
}

type EncodeResult struct {
	Title    string `json:"title"`
	Hash     string `json:"hash"`
	Location string `json:"location"`
}

type encodeDestination struct {
	handler.TypeBasedRouter
}

func (d encodeDestination) Encode(e events.Event[Encode]) error {
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(e.Data.Location)))
	location := strings.ReplaceAll(e.Data.Location, ".mkv", "_encoded.mkv")
	return d.Handler(events.Event[any]{
		Context: e.Context,
		Data: EncodeResult{
			Title:    e.Data.Title,
			Hash:     hash,
			Location: location,
		},
	})
}

// -- Utility Functions --

func async[T any](wrapped events.Handler[T]) events.AsyncHandler[T] {
	return func(e events.Event[T]) {
		go func() {
			err := wrapped(e)
			writeEvent(os.Stderr, e, err)
		}()
	}
}

func delay[T any](d time.Duration, h events.Handler[T]) events.Handler[T] {
	return func(e events.Event[T]) error {
		time.Sleep(d)
		return h(e)
	}
}

func attachHandler[T any](mux *http.ServeMux, path string, handler events.Handler[T]) {
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		status := r.URL.Query().Get("status")
		var data T
		_ = json.NewDecoder(r.Body).Decode(&data)
		event := events.Event[T]{
			Context: events.Context{
				ID:        id,
				Status:    events.Status(status),
				Source:    path,
				Timestamp: time.Now().UTC(),
			},
			Data: data,
		}
		err := handler(event)
		writeEvent(w, event, err)
	})
}

func writeEvent[T any](w io.Writer, event events.Event[T], err error) {
	timestamp := event.Context.Timestamp.Format("03:04:05.000")
	if err != nil {
		fmt.Fprintf(w, "%s %-25s | %-15s %s\n", timestamp, event.Context.Source, event.Context.ID, err.Error())
	} else {
		fmt.Fprintf(w, "%s %-25s | %-15s \n", timestamp, event.Context.Source, event.Context.ID)
	}
}
