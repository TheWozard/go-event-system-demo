# Go Event System Demo

A demonstration of event-driven architecture patterns in Go, simulating a media processing pipeline.

## Overview

This project demonstrates various event system patterns through a media catalog system that processes actors, movies, and TV series data.

There are 3 main components in this project.
1. **Table** - An eventually consistent schema consistent set of data.
2. **Queue** - An event steam.
3. **Handler** - The function that processes events.

> A **Table** differs from a **Queue** in that when an event is received a **Table** can filter the event if no actual change is made or is received out of order. A **Queue** will always process all events.

These are show in mermaid diagrams through the following shapes
```mermaid
flowchart LR
A[(Table)]
B[\Queue\]
C[Handler]
```

> **Handlers** should always receive from a **Queue** so in the event of processing error the data can be reprocessed. A **Table** would require an actual data change to reprocess.

## Scenarios Covered

### 1. Overloaded Queue
```mermaid
flowchart LR
     A[(rawActors)] --> Q[\rawQueue\]
     B[(rawEpisodes)] --> Q
     C[(rawMovies)] --> Q
     D[(rawSeries)] --> Q
     Q --> H[rawHandler]
```
All raw data is dumped into a single queue. This contains multiple schemas of data for each event.

### 2. Recursive Breakdown
```mermaid
flowchart LR
     A[(rawSeries)] -->|1| Q[\rawQueue\]
     B[(rawEpisodes)] -->|4| Q
     Q -->|2| H[rawHandler]
     H -->|3| B
     Q -->|5| H
```
The same queue/handler is used to split an object and process its children.

> This is generally an anti-pattern but can occur if data vendor are inconsistent on how they provide data, requiring multiple levels of raw data to be stored to enable vendor syncing

### 3. Async Secondary Processes
```mermaid
flowchart LR
     A[(rawVideo)] --> Q[\rawQueue\]
     Q --> H[rawHandler]
     H -->|1| V[(standardVideo)]
     H -->|N| E[\encodeData\]
     E --> EH[encodeHandler]
     EH --> ED[(encodedData)]
```
A single input schema contains a subset of N many objects which are slow to process. These are split off so the fast data can be processed quickly

### 4. Fan Out Reconciliation Using Eventual Consistency
```mermaid
flowchart LR
     A[Set of Records 1,3',4']
     A --> B[Record 1]
     A --> C[Record 3']
     A --> D[Record 4']
     B & C & D --> E[(Record Store 1,2,3)]
     E --> F[Record 2 Deleted]
     E --> G[Record 3 Updated]
     E --> H[Record 4 Added]
```
Given an input that contains the complete set, produce both updates and deletes to bring a consumer up to date. This is achieved through eventual consistency timestamp and setting all records before a given point as deleted.

## Running the Demo

```bash
# Start the server
go run main.go

# In another terminal, run the demo script
./demo.sh
```