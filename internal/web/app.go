package web

import (
	"log"
	"net/http"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"

	"auction-site-go/internal/domain"
)

// App represents the web application
type App struct {
	Router         *mux.Router
	State          *AppState
	OnCommand      func(domain.Command) error
	OnEvent        func(domain.Event) error
	GetCurrentTime func() time.Time
}

// NewApp creates a new web application
func NewApp(repo domain.Repository, onCommand func(domain.Command) error, onEvent func(domain.Event) error, getCurrentTime func() time.Time) *App {
	state := NewAppState(repo)
	router := mux.NewRouter()

	app := &App{
		Router:         router,
		State:          state,
		OnCommand:      onCommand,
		OnEvent:        onEvent,
		GetCurrentTime: getCurrentTime,
	}

	app.setupRoutes()

	return app
}

// setupRoutes sets up the HTTP routes
func (a *App) setupRoutes() {
	// Middleware for logging
	a.Router.Use(func(next http.Handler) http.Handler {
		return handlers.LoggingHandler(log.Writer(), next)
	})

	// Routes
	a.Router.HandleFunc("/auctions", getAuctions(a.State)).Methods("GET")
	a.Router.HandleFunc("/auctions/{id}", getAuction(a.State)).Methods("GET")
	a.Router.HandleFunc("/auctions", createAuction(a.State, a.OnCommand, a.OnEvent, a.GetCurrentTime)).Methods("POST")
	a.Router.HandleFunc("/auctions/{id}/bids", placeBid(a.State, a.OnCommand, a.OnEvent, a.GetCurrentTime)).Methods("POST")
}

// Run starts the web server
func (a *App) Run(addr string) error {
	log.Printf("Server listening on %s", addr)
	return http.ListenAndServe(addr, a.Router)
}
