package domain

import (
	"sort"
	"time"
)

// SealedBidOptions defines the options for a sealed bid auction
type SealedBidOptions string

const (
	// Blind is a sealed first-price auction where the highest bidder pays the price they submitted
	Blind SealedBidOptions = "Blind"

	// Vickrey is a sealed second-price auction where the highest bidder pays the second-highest bid
	Vickrey SealedBidOptions = "Vickrey"
)

// SealedBidState represents the state of a sealed bid auction
type SealedBidState struct {
	// bids maps user IDs to their bids
	bids       map[UserId]Bid
	bidsList   []Bid
	disclosing bool
	expiry     time.Time
	options    SealedBidOptions
}

// NewSealedBidState creates a new sealed bid auction state
func NewSealedBidState(expiry time.Time, options SealedBidOptions) *SealedBidState {
	return &SealedBidState{
		bids:       make(map[UserId]Bid),
		bidsList:   []Bid{},
		disclosing: false,
		expiry:     expiry,
		options:    options,
	}
}

// Increment advances the state based on the current time
func (s *SealedBidState) Increment(now time.Time) State {
	// If already disclosing, nothing changes
	if s.disclosing {
		return s
	}

	// Check if we should transition to disclosing state
	if now.After(s.expiry) || now.Equal(s.expiry) {
		// Convert to slice and sort by bid amount (highest first)
		bids := make([]Bid, 0, len(s.bids))
		for _, bid := range s.bids {
			bids = append(bids, bid)
		}

		// Sort bids by amount in descending order
		sort.Slice(bids, func(i, j int) bool {
			return bids[i].Amount > bids[j].Amount
		})

		// Create new state with disclosing = true
		return &SealedBidState{
			bids:       s.bids,
			bidsList:   bids,
			disclosing: true,
			expiry:     s.expiry,
			options:    s.options,
		}
	}

	// No change needed
	return s
}

// AddBid attempts to add a bid to the state
func (s *SealedBidState) AddBid(bid Bid) (State, error) {
	now := bid.At
	auctionId := bid.ForAuction
	userId := bid.Bidder.ID

	// Increment state first to check if it's already ended
	next := s.Increment(now)

	sealedState, ok := next.(*SealedBidState)
	if !ok {
		return next, NewAuctionHasEndedError(auctionId)
	}

	if sealedState.disclosing {
		return sealedState, NewAuctionHasEndedError(auctionId)
	}

	if _, exists := sealedState.bids[userId]; exists {
		return sealedState, NewAlreadyPlacedBidError()
	}

	// Add the bid
	newBids := make(map[UserId]Bid)
	for k, v := range sealedState.bids {
		newBids[k] = v
	}
	newBids[userId] = bid

	// Update bidsList
	newBidsList := make([]Bid, 0, len(newBids))
	for _, b := range newBids {
		newBidsList = append(newBidsList, b)
	}

	return &SealedBidState{
		bids:       newBids,
		bidsList:   newBidsList,
		disclosing: sealedState.disclosing,
		expiry:     sealedState.expiry,
		options:    sealedState.options,
	}, nil
}

// GetBids returns all bids in the state
func (s *SealedBidState) GetBids() []Bid {
	if s.disclosing {
		return s.bidsList
	}

	bids := make([]Bid, 0, len(s.bids))
	for _, bid := range s.bids {
		bids = append(bids, bid)
	}
	return bids
}

// TryGetAmountAndWinner attempts to get the winning amount and bidder
func (s *SealedBidState) TryGetAmountAndWinner() (int64, UserId, bool) {
	if !s.disclosing || len(s.bidsList) == 0 {
		return 0, "", false
	}

	highestBid := s.bidsList[0]

	if s.options == Vickrey {
		if len(s.bidsList) > 1 {
			// Second highest bid price
			return s.bidsList[1].Amount, highestBid.Bidder.ID, true
		}
		// If there's only one bid, the winner pays their own bid
		return highestBid.Amount, highestBid.Bidder.ID, true
	}

	// Blind auction - highest bidder pays their bid
	return highestBid.Amount, highestBid.Bidder.ID, true
}

// HasEnded returns true if the auction has ended
func (s *SealedBidState) HasEnded() bool {
	return s.disclosing
}
