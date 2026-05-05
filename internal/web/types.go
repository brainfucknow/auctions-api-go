package web

import (
	"encoding/json"
	"sync"
	"time"

	"auction-site-go/internal/domain"
)

// AppState holds the application state
type AppState struct {
	auctions *sync.Map // map[domain.AuctionId]struct{Auction domain.Auction, State domain.State}
}

// NewAppState creates a new application state
func NewAppState(repo domain.Repository) *AppState {
	auctions := &sync.Map{}

	// Convert repository to sync.Map
	for id, entry := range repo {
		auctions.Store(id, entry)
	}

	return &AppState{
		auctions: auctions,
	}
}

// GetRepository returns the current repository
func (s *AppState) GetRepository() domain.Repository {
	repo := make(domain.Repository)

	s.auctions.Range(func(key, value interface{}) bool {
		id := key.(domain.AuctionId)
		entry := value.(struct {
			Auction domain.Auction
			State   domain.State
		})
		repo[id] = entry
		return true
	})

	return repo
}

// UpdateRepository updates the repository with new values
func (s *AppState) UpdateRepository(repo domain.Repository) {
	for id, entry := range repo {
		s.auctions.Store(id, entry)
	}
}

// ApiError represents an API error response
type ApiError struct {
	Message string `json:"message"`
}

// BidRequest represents a request to place a bid
type BidRequest struct {
	Amount int64 `json:"amount"`
}

// AddAuctionRequest represents a request to add an auction
type AddAuctionRequest struct {
	ID       domain.AuctionId   `json:"id"`
	StartsAt time.Time          `json:"startsAt"`
	Title    string             `json:"title"`
	EndsAt   time.Time          `json:"endsAt"`
	Currency domain.Currency    `json:"currency"`
	Type     domain.AuctionType `json:"typ,omitempty"`
}

// UnmarshalJSON implements json.Unmarshaler
func (r *AddAuctionRequest) UnmarshalJSON(data []byte) error {
	// Define a temporary struct with same fields but without methods
	type TempReq AddAuctionRequest

	// Default values
	temp := TempReq{
		Currency: domain.VAC,
	}

	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	*r = AddAuctionRequest(temp)
	return nil
}

// AuctionBidResponse represents a bid in an auction response
type AuctionBidResponse struct {
	Amount int64       `json:"amount"`
	Bidder domain.User `json:"bidder"`
}

// AuctionResponse represents an auction with bids and winner information
type AuctionResponse struct {
	ID          domain.AuctionId     `json:"id"`
	StartsAt    time.Time            `json:"startsAt"`
	Title       string               `json:"title"`
	Expiry      time.Time            `json:"expiry"`
	Currency    domain.Currency      `json:"currency"`
	Bids        []AuctionBidResponse `json:"bids"`
	Winner      *domain.UserId       `json:"winner"`
	WinnerPrice *int64               `json:"winnerPrice"`
}

// AuctionListItem represents an auction in a list
type AuctionListItem struct {
	ID       domain.AuctionId `json:"id"`
	StartsAt time.Time        `json:"startsAt"`
	Title    string           `json:"title"`
	Expiry   time.Time        `json:"expiry"`
	Currency domain.Currency  `json:"currency"`
}
