package domain

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// TimedAscendingOptions defines the options for a timed ascending auction
type TimedAscendingOptions struct {
	// The seller has set a minimum sale price in advance (the 'reserve' price)
	// If the final bid does not reach that price, the item remains unsold
	ReservePrice int64 `json:"reservePrice"`

	// The minimum amount by which the next bid must exceed the current highest bid
	MinRaise int64 `json:"minRaise"`

	// If no competing bidder challenges the standing bid within a given time frame,
	// the standing bid becomes the winner
	TimeFrame time.Duration `json:"timeFrame"`
}

// String returns a string representation of the options
func (o TimedAscendingOptions) String() string {
	seconds := int(o.TimeFrame.Seconds())
	return fmt.Sprintf("English|%d|%d|%d", o.ReservePrice, o.MinRaise, seconds)
}

// ParseTimedAscendingOptions parses a string into TimedAscendingOptions
func ParseTimedAscendingOptions(s string) (*TimedAscendingOptions, error) {
	// Split the string by '|'
	parts := strings.Split(s, "|")
	if len(parts) != 4 || parts[0] != "English" {
		return nil, fmt.Errorf("invalid timed ascending options format: %s", s)
	}

	// Parse reserve price
	reserveAmount, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid reserve price format: %s", parts[1])
	}

	// Parse min raise
	minRaiseAmount, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid min raise format: %s", parts[2])
	}

	// Parse seconds
	seconds, err := strconv.Atoi(parts[3])
	if err != nil {
		return nil, fmt.Errorf("invalid time frame format: %s", parts[3])
	}

	return &TimedAscendingOptions{
		ReservePrice: reserveAmount,
		MinRaise:     minRaiseAmount,
		TimeFrame:    time.Duration(seconds) * time.Second,
	}, nil
}

// DefaultTimedAscendingOptions creates default options
func DefaultTimedAscendingOptions() TimedAscendingOptions {
	return TimedAscendingOptions{
		ReservePrice: 0,
		MinRaise:     0,
		TimeFrame:    0,
	}
}

// TimedAscendingState represents one of the states of a timed ascending auction
type TimedAscendingState interface {
	State
	isTimedAscendingState()
}

// AwaitingStartState represents a timed ascending auction that hasn't started yet
type AwaitingStartState struct {
	start          time.Time
	startingExpiry time.Time
	options        TimedAscendingOptions
}

func (s *AwaitingStartState) isTimedAscendingState() {}

// OngoingState represents a timed ascending auction that is currently active
type OngoingState struct {
	bids       []Bid
	nextExpiry time.Time
	options    TimedAscendingOptions
}

func (s *OngoingState) isTimedAscendingState() {}

// EndedState represents a timed ascending auction that has ended
type EndedState struct {
	bids    []Bid
	expiry  time.Time
	options TimedAscendingOptions
}

func (s *EndedState) isTimedAscendingState() {}

// NewTimedAscendingState creates a new timed ascending auction state
func NewTimedAscendingState(start, expiry time.Time, options TimedAscendingOptions) TimedAscendingState {
	return &AwaitingStartState{
		start:          start,
		startingExpiry: expiry,
		options:        options,
	}
}

// Increment advances the AwaitingStartState based on the current time
func (s *AwaitingStartState) Increment(now time.Time) State {
	if now.After(s.start) {
		if now.Before(s.startingExpiry) {
			// Transition to OngoingState
			return &OngoingState{
				bids:       []Bid{},
				nextExpiry: s.startingExpiry,
				options:    s.options,
			}
		}
		// Transition directly to EndedState
		return &EndedState{
			bids:    []Bid{},
			expiry:  s.startingExpiry,
			options: s.options,
		}
	}
	// Stay in AwaitingStartState
	return s
}

// AddBid attempts to add a bid to the AwaitingStartState
func (s *AwaitingStartState) AddBid(bid Bid) (State, error) {
	next := s.Increment(bid.At)
	if _, ok := next.(*AwaitingStartState); ok {
		return next, NewAuctionHasNotStartedError(bid.ForAuction)
	}
	return next.AddBid(bid)
}

// GetBids returns all bids in the AwaitingStartState
func (s *AwaitingStartState) GetBids() []Bid {
	return []Bid{}
}

// TryGetAmountAndWinner attempts to get the winning amount and bidder
func (s *AwaitingStartState) TryGetAmountAndWinner() (int64, UserId, bool) {
	return 0, "", false
}

// HasEnded returns true if the auction has ended
func (s *AwaitingStartState) HasEnded() bool {
	return false
}

// Increment advances the OngoingState based on the current time
func (s *OngoingState) Increment(now time.Time) State {
	if now.After(s.nextExpiry) || now.Equal(s.nextExpiry) {
		// Transition to EndedState
		return &EndedState{
			bids:    s.bids,
			expiry:  s.nextExpiry,
			options: s.options,
		}
	}
	// Stay in OngoingState
	return s
}

// AddBid attempts to add a bid to the OngoingState
func (s *OngoingState) AddBid(bid Bid) (State, error) {
	now := bid.At
	bidAmount := bid.Amount

	next := s.Increment(now)
	if _, ok := next.(*EndedState); ok {
		return next, NewAuctionHasEndedError(bid.ForAuction)
	}

	// We're still in OngoingState
	newExpiry := s.nextExpiry
	if now.Add(s.options.TimeFrame).After(newExpiry) {
		newExpiry = now.Add(s.options.TimeFrame)
	}

	if len(s.bids) == 0 {
		// First bid is always accepted
		return &OngoingState{
			bids:       append([]Bid{bid}, s.bids...),
			nextExpiry: newExpiry,
			options:    s.options,
		}, nil
	}

	// Check if bid is higher than the current highest bid + minimum raise
	highestBid := s.bids[0]
	highestAmount := highestBid.Amount
	minRaiseAmount := s.options.MinRaise

	// Calculate minimum acceptable bid
	minAcceptableBid := highestAmount + minRaiseAmount

	// Changed comparison from <= to <, and using >= for the check
	if bidAmount >= minAcceptableBid {
		// Bid is acceptable
		return &OngoingState{
			bids:       append([]Bid{bid}, s.bids...),
			nextExpiry: newExpiry,
			options:    s.options,
		}, nil
	}

	return s, NewMustPlaceBidOverHighestError(highestAmount)
}

// GetBids returns all bids in the OngoingState
func (s *OngoingState) GetBids() []Bid {
	return s.bids
}

// TryGetAmountAndWinner attempts to get the winning amount and bidder
func (s *OngoingState) TryGetAmountAndWinner() (int64, UserId, bool) {
	return 0, "", false
}

// HasEnded returns true if the auction has ended
func (s *OngoingState) HasEnded() bool {
	return false
}

// Increment advances the EndedState based on the current time
func (s *EndedState) Increment(now time.Time) State {
	// EndedState doesn't change
	return s
}

// AddBid attempts to add a bid to the EndedState
func (s *EndedState) AddBid(bid Bid) (State, error) {
	return s, NewAuctionHasEndedError(bid.ForAuction)
}

// GetBids returns all bids in the EndedState
func (s *EndedState) GetBids() []Bid {
	return s.bids
}

// TryGetAmountAndWinner attempts to get the winning amount and bidder
func (s *EndedState) TryGetAmountAndWinner() (int64, UserId, bool) {
	if len(s.bids) == 0 {
		return 0, "", false
	}

	highestBid := s.bids[0]

	// Check if highest bid exceeds reserve price
	if highestBid.Amount > s.options.ReservePrice {
		return highestBid.Amount, highestBid.Bidder.ID, true
	}

	return 0, "", false
}

// HasEnded returns true if the auction has ended
func (s *EndedState) HasEnded() bool {
	return true
}
