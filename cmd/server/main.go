package main

import (
	"log"
	"os"
	"path/filepath"
	"time"

	"auction-site-go/internal/domain"
	"auction-site-go/internal/persistence"
	"auction-site-go/internal/web"
)

func main() {
	// Get file paths from environment variables or use defaults
	log.Println("Reading configuration from environment variables")
	eventsFile := os.Getenv("EVENTS_FILE")
	if eventsFile == "" {
		eventsFile = "tmp/events.jsonl"
	}

	commandsFile := os.Getenv("COMMANDS_FILE")
	if commandsFile == "" {
		commandsFile = "tmp/commands.jsonl"
	}

	// Get server port from environment variables or use default
	port := os.Getenv("SERVER_PORT")
	if port == "" {
		port = "8080"
	}

	// Ensure directory exists
	log.Printf("Ensuring directory exists for events file: %s", eventsFile)
	dir := filepath.Dir(eventsFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatalf("Failed to create directory: %v", err)
	}

	// Read events
	events, err := persistence.ReadEvents(eventsFile)
	if err != nil {
		log.Fatalf("Failed to read events: %v", err)
	}

	// Initialize repository
	repo := domain.EventsToAuctionStates(events)

	onCommand := func(command domain.Command) error {
		return persistence.WriteCommands(commandsFile, []domain.Command{command})
	}

	// Event handler
	onEvent := func(event domain.Event) error {
		return persistence.WriteEvents(eventsFile, []domain.Event{event})
	}

	// Get current time
	getCurrentTime := time.Now

	// Create web application
	app := web.NewApp(repo, onCommand, onEvent, getCurrentTime)

	// Start server
	log.Printf("Starting server on port %s", port)
	log.Fatal(app.Run(":" + port))
}
