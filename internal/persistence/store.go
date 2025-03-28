package persistence

import (
	"auction-site-go/internal/domain"
)

// Store defines the interface for persistence operations
type Store interface {
	// ReadCommands reads all commands from the store
	ReadCommands() ([]domain.Command, error)
	
	// WriteCommands writes commands to the store
	WriteCommands(commands []domain.Command) error
	
	// ReadEvents reads all events from the store
	ReadEvents() ([]domain.Event, error)
	
	// WriteEvents writes events to the store
	WriteEvents(events []domain.Event) error
}

// FileStore implements the Store interface using JSON files
type FileStore struct {
	CommandsPath string
	EventsPath   string
}

// NewFileStore creates a new file-based store
func NewFileStore(commandsPath, eventsPath string) *FileStore {
	return &FileStore{
		CommandsPath: commandsPath,
		EventsPath:   eventsPath,
	}
}

// ReadCommands reads commands from a JSON file
func (s *FileStore) ReadCommands() ([]domain.Command, error) {
	return ReadCommands(s.CommandsPath)
}

// WriteCommands writes commands to a JSON file
func (s *FileStore) WriteCommands(commands []domain.Command) error {
	return WriteCommands(s.CommandsPath, commands)
}

// ReadEvents reads events from a JSON file
func (s *FileStore) ReadEvents() ([]domain.Event, error) {
	return ReadEvents(s.EventsPath)
}

// WriteEvents writes events to a JSON file
func (s *FileStore) WriteEvents(events []domain.Event) error {
	return WriteEvents(s.EventsPath, events)
}
