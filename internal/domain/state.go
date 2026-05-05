package domain

import (
	"time"
)

// State defines the interface for auction states
type State interface {
	// Increment advances the state based on the current time
	Increment(now time.Time) State

	// AddBid attempts to add a bid to the state
	// Returns the next state and an error if the bid cannot be added
	AddBid(bid Bid) (State, error)

	// GetBids returns all bids in the state
	GetBids() []Bid

	// TryGetAmountAndWinner attempts to get the winning amount and bidder
	// Returns the amount, the winner's ID, and whether a winner was found
	TryGetAmountAndWinner() (int64, UserId, bool)

	// HasEnded returns true if the auction has ended
	HasEnded() bool
}
