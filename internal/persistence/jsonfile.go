package persistence

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"auction-site-go/internal/domain"
)

// ReadCommands reads commands from a JSON file (legacy function, use Store interface instead)
func ReadCommands(path string) ([]domain.Command, error) {
	exists, err := fileExists(path)
	if err != nil {
		return nil, err
	}
	if !exists {
		return []domain.Command{}, nil
	}

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(data), "\n")
	commands := make([]domain.Command, 0, len(lines))

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		cmd, err := domain.UnmarshalCommand([]byte(line))
		if err != nil {
			return nil, fmt.Errorf("error unmarshaling command: %v", err)
		}

		commands = append(commands, cmd)
	}

	return commands, nil
}

// WriteCommands writes commands to a JSON file
func WriteCommands(path string, commands []domain.Command) error {
	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Check if file exists
	exists, err := fileExists(path)
	if err != nil {
		return err
	}

	var file *os.File
	if !exists {
		file, err = os.Create(path)
	} else {
		file, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	}
	if err != nil {
		return err
	}
	defer file.Close()

	// Write commands
	for _, cmd := range commands {
		data, err := json.Marshal(cmd)
		if err != nil {
			return fmt.Errorf("error marshaling command: %v", err)
		}

		if exists {
			_, err = file.WriteString("\n")
			if err != nil {
				return err
			}
		}

		_, err = file.Write(data)
		if err != nil {
			return err
		}

		exists = true
	}

	return nil
}

// ReadEvents reads events from a JSON file
func ReadEvents(path string) ([]domain.Event, error) {
	exists, err := fileExists(path)
	if err != nil {
		return nil, err
	}
	if !exists {
		return []domain.Event{}, nil
	}

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(data), "\n")
	events := make([]domain.Event, 0, len(lines))

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		event, err := domain.UnmarshalEvent([]byte(line))
		if err != nil {
			return nil, fmt.Errorf("error unmarshaling event: %v", err)
		}

		events = append(events, event)
	}

	return events, nil
}

// WriteEvents writes events to a JSON file
func WriteEvents(path string, events []domain.Event) error {
	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Check if file exists
	exists, err := fileExists(path)
	if err != nil {
		return err
	}

	var file *os.File
	if !exists {
		file, err = os.Create(path)
	} else {
		file, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	}
	if err != nil {
		return err
	}
	defer file.Close()

	// Write events
	for _, event := range events {
		data, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("error marshaling event: %v", err)
		}

		if exists {
			_, err = file.WriteString("\n")
			if err != nil {
				return err
			}
		}

		_, err = file.Write(data)
		if err != nil {
			return err
		}

		exists = true
	}

	return nil
}

// fileExists checks if a file exists
func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
