package persistence

import (
	"auction-site-go/internal/domain"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
)

// PostgresConfig holds configuration for PostgreSQL connection
type PostgresConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
}

// PostgresStore implements event storage using PostgreSQL
type PostgresStore struct {
	db *sql.DB
}

// NewPostgresStore creates a new PostgreSQL store
func NewPostgresStore(config PostgresConfig) (*PostgresStore, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		config.Host, config.Port, config.User, config.Password, config.DBName, config.SSLMode,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %v", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping PostgreSQL: %v", err)
	}

	// Create tables if they don't exist
	if err := createTables(db); err != nil {
		return nil, fmt.Errorf("failed to create tables: %v", err)
	}

	return &PostgresStore{db: db}, nil
}

// createTables creates the necessary tables if they don't exist
func createTables(db *sql.DB) error {
	// Create commands table
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS commands (
			id SERIAL PRIMARY KEY,
			type VARCHAR(50) NOT NULL,
			timestamp TIMESTAMP NOT NULL,
			data JSONB NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return err
	}

	// Create events table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS events (
			id SERIAL PRIMARY KEY,
			type VARCHAR(50) NOT NULL,
			timestamp TIMESTAMP NOT NULL,
			data JSONB NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT NOW()
		)
	`)
	return err
}

// Close closes the database connection
func (s *PostgresStore) Close() error {
	return s.db.Close()
}

// ReadCommands reads commands from the PostgreSQL database
func (s *PostgresStore) ReadCommands() ([]domain.Command, error) {
	rows, err := s.db.Query(`
		SELECT type, data FROM commands
		ORDER BY timestamp ASC, id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query commands: %v", err)
	}
	defer rows.Close()

	commands := []domain.Command{}
	for rows.Next() {
		var cmdType string
		var data []byte

		if err := rows.Scan(&cmdType, &data); err != nil {
			return nil, fmt.Errorf("failed to scan command row: %v", err)
		}

		// Construct JSON with type field
		jsonData := fmt.Sprintf(`{"$type":"%s",%s}`, cmdType, string(data)[1:])
		cmd, err := domain.UnmarshalCommand([]byte(jsonData))
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal command: %v", err)
		}

		commands = append(commands, cmd)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating command rows: %v", err)
	}

	return commands, nil
}

// WriteCommands writes commands to the PostgreSQL database
func (s *PostgresStore) WriteCommands(commands []domain.Command) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO commands (type, timestamp, data)
		VALUES ($1, $2, $3)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	for _, cmd := range commands {
		// Marshal command to JSON
		data, err := json.Marshal(cmd)
		if err != nil {
			return fmt.Errorf("failed to marshal command: %v", err)
		}

		// Extract command type and timestamp
		var typeCheck struct {
			Type string    `json:"$type"`
			Time time.Time `json:"at"`
		}
		if err := json.Unmarshal(data, &typeCheck); err != nil {
			return fmt.Errorf("failed to extract command type: %v", err)
		}

		// Remove the $type field from the JSON for storage
		var rawData map[string]interface{}
		if err := json.Unmarshal(data, &rawData); err != nil {
			return fmt.Errorf("failed to parse command data: %v", err)
		}
		delete(rawData, "$type")
		
		// Re-marshal without the $type field
		cleanData, err := json.Marshal(rawData)
		if err != nil {
			return fmt.Errorf("failed to re-marshal command data: %v", err)
		}

		// Execute insert
		_, err = stmt.Exec(typeCheck.Type, typeCheck.Time, cleanData)
		if err != nil {
			return fmt.Errorf("failed to insert command: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}

// ReadEvents reads events from the PostgreSQL database
func (s *PostgresStore) ReadEvents() ([]domain.Event, error) {
	rows, err := s.db.Query(`
		SELECT type, data FROM events
		ORDER BY timestamp ASC, id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query events: %v", err)
	}
	defer rows.Close()

	events := []domain.Event{}
	for rows.Next() {
		var eventType string
		var data []byte

		if err := rows.Scan(&eventType, &data); err != nil {
			return nil, fmt.Errorf("failed to scan event row: %v", err)
		}

		// Construct JSON with type field
		jsonData := fmt.Sprintf(`{"$type":"%s",%s}`, eventType, string(data)[1:])
		event, err := domain.UnmarshalEvent([]byte(jsonData))
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal event: %v", err)
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating event rows: %v", err)
	}

	return events, nil
}

// WriteEvents writes events to the PostgreSQL database
func (s *PostgresStore) WriteEvents(events []domain.Event) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO events (type, timestamp, data)
		VALUES ($1, $2, $3)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	for _, event := range events {
		// Marshal event to JSON
		data, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("failed to marshal event: %v", err)
		}

		// Extract event type and timestamp
		var typeCheck struct {
			Type string    `json:"$type"`
			Time time.Time `json:"at"`
		}
		if err := json.Unmarshal(data, &typeCheck); err != nil {
			return fmt.Errorf("failed to extract event type: %v", err)
		}

		// Remove the $type field from the JSON for storage
		var rawData map[string]interface{}
		if err := json.Unmarshal(data, &rawData); err != nil {
			return fmt.Errorf("failed to parse event data: %v", err)
		}
		delete(rawData, "$type")
		
		// Re-marshal without the $type field
		cleanData, err := json.Marshal(rawData)
		if err != nil {
			return fmt.Errorf("failed to re-marshal event data: %v", err)
		}

		// Execute insert
		_, err = stmt.Exec(typeCheck.Type, typeCheck.Time, cleanData)
		if err != nil {
			return fmt.Errorf("failed to insert event: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}
