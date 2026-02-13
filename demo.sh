#!/bin/bash

BASE_URL="http://localhost:8080"
DELAY="${DELAY:-0.1}"

# Cleanup
rm -rf ./data
mkdir -p ./data

# Shorthand
update() {
  curl -s -X POST "${BASE_URL}/${1}?id=${2}&status=update" \
    -H "Content-Type: application/json" \
    -d "$3"
  sleep "$DELAY"
}

delete() {
  curl -s -X POST "${BASE_URL}/${1}?id=${2}&status=delete" \
    -H "Content-Type: application/json" \
    -d "$3"
  sleep "$DELAY"
}

# --- Simulation ---

echo "Setup initial data..."
update rawActors actor-1 '{"name": "Tom Hanks", "nationality": "American"}'
update rawActors actor-2 '{"name": "Meryl Streep", "nationality": "American"}'
update rawActors actor-3 '{"name": "Leonardo DiCaprio", "nationality": "American"}'

update rawMovies movie-1 '{"title": "Inception", "director": "Christopher Nolan", "year": 2010}'
update rawMovies movie-2 '{"title": "The Matrix", "director": "The Wachowskis", "year": 1999}'

update rawSeries series-1 '{"title": "Breaking Bad", "episodes": [{"title": "Pilot"}, {"title": "Cats in the Bag"}]}'
update rawSeries series-2 '{"title": "The Office", "episodes": [{"title": "Pilot"}, {"title": "Diversity Day"}, {"title": "Health Care"}]}'

echo "Complete sets can delete unused records"
# update rawSeries series-1 '{"title": "Breaking Bad", "episodes": [{"title": "Pilot"}, {"title": "Cats in the Bag"}, {"title": "And the Bags in the River"}]}'
# update rawSeries series-1 '{"title": "Breaking Bad", "episodes": [{"title": "Pilot"}, {"title": "Cats in the Bag"}]}'

echo "Performing filtered updates..."
update rawActors actor-2 '{"name": "Meryl Streep", "nationality": "American"}'