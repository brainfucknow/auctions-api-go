package domain

import (
	"encoding/json"
	"fmt"
	"strings"
)

// UserId is a unique identifier for a user
type UserId string

// AuctionId is a unique identifier for an auction
type AuctionId int64

// User represents either a buyer/seller or support user
type User struct {
	ID   UserId `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"` // "BuyerOrSeller" or "Support"
}

// NewBuyerOrSeller creates a new buyer or seller user
func NewBuyerOrSeller(id UserId, name string) User {
	return User{
		ID:   id,
		Name: name,
		Type: "BuyerOrSeller",
	}
}

// NewSupport creates a new support user
func NewSupport(id UserId) User {
	return User{
		ID:   id,
		Type: "Support",
	}
}

// MarshalJSON implements json.Marshaler interface
func (u User) MarshalJSON() ([]byte, error) {
	var s string
	if u.Type == "BuyerOrSeller" {
		s = fmt.Sprintf("BuyerOrSeller|%s|%s", u.ID, u.Name)
	} else {
		s = fmt.Sprintf("Support|%s", u.ID)
	}
	return json.Marshal(s)
}

// UnmarshalJSON implements json.Unmarshaler interface
func (u *User) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	parts := strings.Split(s, "|")
	if len(parts) == 0 {
		return fmt.Errorf("invalid user format: %s", s)
	}

	u.Type = parts[0]
	if u.Type == "BuyerOrSeller" {
		if len(parts) != 3 {
			return fmt.Errorf("invalid BuyerOrSeller format: %s", s)
		}
		u.ID = UserId(parts[1])
		u.Name = parts[2]
	} else if u.Type == "Support" {
		if len(parts) != 2 {
			return fmt.Errorf("invalid Support format: %s", s)
		}
		u.ID = UserId(parts[1])
	} else {
		return fmt.Errorf("unknown user type: %s", u.Type)
	}

	return nil
}

// Domain errors
type ErrorType string

const (
	ErrorUnknownAuction          ErrorType = "UnknownAuction"
	ErrorAuctionAlreadyExists    ErrorType = "AuctionAlreadyExists"
	ErrorAuctionHasEnded         ErrorType = "AuctionHasEnded"
	ErrorAuctionHasNotStarted    ErrorType = "AuctionHasNotStarted"
	ErrorSellerCannotPlaceBids   ErrorType = "SellerCannotPlaceBids"
	ErrorInvalidUserData         ErrorType = "InvalidUserData"
	ErrorMustPlaceBidOverHighest ErrorType = "MustPlaceBidOverHighestBid"
	ErrorAlreadyPlacedBid        ErrorType = "AlreadyPlacedBid"
)

// DomainError represents an error in the domain
type DomainError struct {
	Type    ErrorType
	Message string
	Data    interface{}
}

func (e DomainError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("%s: %s", e.Type, e.Message)
	}
	return string(e.Type)
}

// NewUnknownAuctionError creates a new UnknownAuction error
func NewUnknownAuctionError(id AuctionId) error {
	return DomainError{
		Type: ErrorUnknownAuction,
		Data: id,
	}
}

// NewAuctionAlreadyExistsError creates a new AuctionAlreadyExists error
func NewAuctionAlreadyExistsError(id AuctionId) error {
	return DomainError{
		Type: ErrorAuctionAlreadyExists,
		Data: id,
	}
}

// NewAuctionHasEndedError creates a new AuctionHasEnded error
func NewAuctionHasEndedError(id AuctionId) error {
	return DomainError{
		Type: ErrorAuctionHasEnded,
		Data: id,
	}
}

// NewAuctionHasNotStartedError creates a new AuctionHasNotStarted error
func NewAuctionHasNotStartedError(id AuctionId) error {
	return DomainError{
		Type: ErrorAuctionHasNotStarted,
		Data: id,
	}
}

// NewSellerCannotPlaceBidsError creates a new SellerCannotPlaceBids error
func NewSellerCannotPlaceBidsError(userId UserId, auctionId AuctionId) error {
	return DomainError{
		Type: ErrorSellerCannotPlaceBids,
		Data: map[string]interface{}{
			"userId":    userId,
			"auctionId": auctionId,
		},
	}
}

// NewInvalidUserDataError creates a new InvalidUserData error
func NewInvalidUserDataError(message string) error {
	return DomainError{
		Type:    ErrorInvalidUserData,
		Message: message,
	}
}

// NewMustPlaceBidOverHighestError creates a new MustPlaceBidOverHighest error
func NewMustPlaceBidOverHighestError(amount int64) error {
	return DomainError{
		Type: ErrorMustPlaceBidOverHighest,
		Data: amount,
	}
}

// NewAlreadyPlacedBidError creates a new AlreadyPlacedBid error
func NewAlreadyPlacedBidError() error {
	return DomainError{
		Type: ErrorAlreadyPlacedBid,
	}
}
