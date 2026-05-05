package domain

import (
	"encoding/json"
	"fmt"
	"time"
)

// AuctionTypeEnum represents the type of auction as an enum
type AuctionTypeEnum int

const (
	TimedAscending  AuctionTypeEnum = iota
	SingleSealedBid                 = 1
)

// String returns the string representation of the auction type enum
func (t AuctionTypeEnum) String() string {
	switch t {
	case TimedAscending:
		return "TimedAscending"
	case SingleSealedBid:
		return "SingleSealedBid"
	default:
		return "Unknown"
	}
}

// AuctionType represents the type of auction
type AuctionType struct {
	Type    AuctionTypeEnum `json:"type"`
	Options string          `json:"options"`
}

// NewTimedAscendingType creates a new TimedAscending auction type
func NewTimedAscendingType(options TimedAscendingOptions) AuctionType {
	return AuctionType{
		Type:    TimedAscending,
		Options: options.String(),
	}
}

// NewSingleSealedBidType creates a new SingleSealedBid auction type
func NewSingleSealedBidType(options SealedBidOptions) AuctionType {
	return AuctionType{
		Type:    SingleSealedBid,
		Options: string(options),
	}
}

// String returns a string representation of the auction type
func (t AuctionType) String() string {
	return t.Options
}

// UnmarshalJSON implements json.Unmarshaler interface
func (t *AuctionType) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	// Parse the options
	if len(s) >= 7 && s[:7] == "English" {
		options, err := ParseTimedAscendingOptions(s)
		if err != nil {
			return err
		}
		t.Type = TimedAscending
		t.Options = options.String()
	} else if s == "Vickrey" || s == "Blind" {
		t.Type = SingleSealedBid
		t.Options = s
	} else {
		return fmt.Errorf("unknown auction type: %s", s)
	}

	return nil
}

// MarshalJSON implements json.Marshaler interface
func (t AuctionType) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Options)
}

// Auction represents an auction
type Auction struct {
	ID       AuctionId   `json:"id"`
	StartsAt time.Time   `json:"startsAt"`
	Title    string      `json:"title"`
	Expiry   time.Time   `json:"expiry"`
	Seller   User        `json:"user"`
	Type     AuctionType `json:"type"`
	Currency Currency    `json:"currency"`
}

// NewAuction creates a new auction
func NewAuction(id AuctionId, startsAt time.Time, title string, expiry time.Time, seller User, auctionType AuctionType, currency Currency) Auction {
	return Auction{
		ID:       id,
		StartsAt: startsAt,
		Title:    title,
		Expiry:   expiry,
		Seller:   seller,
		Type:     auctionType,
		Currency: currency,
	}
}

// ValidateBid validates a bid for the auction
func (a Auction) ValidateBid(bid Bid) error {
	if bid.Bidder.ID == a.Seller.ID {
		return NewSellerCannotPlaceBidsError(bid.Bidder.ID, a.ID)
	}

	return nil
}

// CreateEmptyState creates a new state for the auction
func (a Auction) CreateEmptyState() State {
	if a.Type.Type == SingleSealedBid {
		options := SealedBidOptions(a.Type.Options)
		return NewSealedBidState(a.Expiry, options)
	} else if a.Type.Type == TimedAscending {
		options, err := ParseTimedAscendingOptions(a.Type.Options)
		if err != nil {
			// Fall back to default options if parsing fails
			defaultOptions := DefaultTimedAscendingOptions()
			return NewTimedAscendingState(a.StartsAt, a.Expiry, defaultOptions)
		}
		return NewTimedAscendingState(a.StartsAt, a.Expiry, *options)
	}

	// Default to a sealed bid auction if the type is unknown
	return NewSealedBidState(a.Expiry, Blind)
}
