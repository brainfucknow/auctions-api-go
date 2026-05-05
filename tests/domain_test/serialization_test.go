package domain_test

import (
	"encoding/json"
	"testing"
	"time"

	"auction-site-go/internal/domain"
)

// Test serialization and deserialization of commands and events
func TestCommandAndEventSerialization(t *testing.T) {
	// Sample data
	now := time.Now().UTC().Truncate(time.Millisecond) // truncate to avoid precision issues
	auctionId := domain.AuctionId(1)
	seller := domain.NewBuyerOrSeller("seller1", "Seller 1")
	buyer := domain.NewBuyerOrSeller("buyer1", "Buyer 1")

	// Create auction type
	options := domain.DefaultTimedAscendingOptions()
	auctionType := domain.NewTimedAscendingType(options)

	// Create auction
	auction := domain.Auction{
		ID:       auctionId,
		StartsAt: now,
		Title:    "Test Auction",
		Expiry:   now.Add(24 * time.Hour),
		Seller:   seller,
		Type:     auctionType,
		Currency: domain.VAC,
	}

	// Create bid
	bid := domain.Bid{
		ForAuction: auctionId,
		Bidder:     buyer,
		At:         now.Add(time.Hour),
		Amount:     10,
	}

	// Test AddAuctionCommand serialization
	t.Run("AddAuctionCommandSerialization", func(t *testing.T) {
		cmd := domain.AddAuctionCommand{
			Time:    now,
			Auction: auction,
		}

		data, err := json.Marshal(cmd)
		if err != nil {
			t.Fatalf("Failed to marshal AddAuctionCommand: %v", err)
		}

		var parsed interface{}
		err = json.Unmarshal(data, &parsed)
		if err != nil {
			t.Fatalf("Failed to unmarshal JSON: %v", err)
		}

		// Check that the JSON contains the expected type
		parsedMap := parsed.(map[string]interface{})
		if parsedMap["$type"] != "AddAuction" {
			t.Errorf("Expected $type to be AddAuction, got %v", parsedMap["$type"])
		}

		// Parse back to command
		parsedCmd, err := domain.UnmarshalCommand(data)
		if err != nil {
			t.Fatalf("Failed to unmarshal AddAuctionCommand: %v", err)
		}

		// Check that we got the right type
		if addCmd, ok := parsedCmd.(domain.AddAuctionCommand); !ok {
			t.Errorf("Expected AddAuctionCommand, got %T", parsedCmd)
		} else {
			// Check a few key fields
			if addCmd.Auction.ID != auctionId {
				t.Errorf("Expected auction ID %v, got %v", auctionId, addCmd.Auction.ID)
			}
			if !addCmd.Time.Equal(now) {
				t.Errorf("Expected time %v, got %v", now, addCmd.Time)
			}
		}
	})

	// Test PlaceBidCommand serialization
	t.Run("PlaceBidCommandSerialization", func(t *testing.T) {
		cmd := domain.PlaceBidCommand{
			Time: now,
			Bid:  bid,
		}

		data, err := json.Marshal(cmd)
		if err != nil {
			t.Fatalf("Failed to marshal PlaceBidCommand: %v", err)
		}

		var parsed interface{}
		err = json.Unmarshal(data, &parsed)
		if err != nil {
			t.Fatalf("Failed to unmarshal JSON: %v", err)
		}

		// Check that the JSON contains the expected type
		parsedMap := parsed.(map[string]interface{})
		if parsedMap["$type"] != "PlaceBid" {
			t.Errorf("Expected $type to be PlaceBid, got %v", parsedMap["$type"])
		}

		// Parse back to command
		parsedCmd, err := domain.UnmarshalCommand(data)
		if err != nil {
			t.Fatalf("Failed to unmarshal PlaceBidCommand: %v", err)
		}

		// Check that we got the right type
		if bidCmd, ok := parsedCmd.(domain.PlaceBidCommand); !ok {
			t.Errorf("Expected PlaceBidCommand, got %T", parsedCmd)
		} else {
			// Check a few key fields
			if bidCmd.Bid.ForAuction != auctionId {
				t.Errorf("Expected auction ID %v, got %v", auctionId, bidCmd.Bid.ForAuction)
			}
			if !bidCmd.Time.Equal(now) {
				t.Errorf("Expected time %v, got %v", now, bidCmd.Time)
			}
		}
	})

	// Test AuctionAddedEvent serialization
	t.Run("AuctionAddedEventSerialization", func(t *testing.T) {
		event := domain.AuctionAddedEvent{
			Time:    now,
			Auction: auction,
		}

		data, err := json.Marshal(event)
		if err != nil {
			t.Fatalf("Failed to marshal AuctionAddedEvent: %v", err)
		}

		var parsed interface{}
		err = json.Unmarshal(data, &parsed)
		if err != nil {
			t.Fatalf("Failed to unmarshal JSON: %v", err)
		}

		// Check that the JSON contains the expected type
		parsedMap := parsed.(map[string]interface{})
		if parsedMap["$type"] != "AuctionAdded" {
			t.Errorf("Expected $type to be AuctionAdded, got %v", parsedMap["$type"])
		}

		// Parse back to event
		parsedEvent, err := domain.UnmarshalEvent(data)
		if err != nil {
			t.Fatalf("Failed to unmarshal AuctionAddedEvent: %v", err)
		}

		// Check that we got the right type
		if addEvent, ok := parsedEvent.(domain.AuctionAddedEvent); !ok {
			t.Errorf("Expected AuctionAddedEvent, got %T", parsedEvent)
		} else {
			// Check a few key fields
			if addEvent.Auction.ID != auctionId {
				t.Errorf("Expected auction ID %v, got %v", auctionId, addEvent.Auction.ID)
			}
			if !addEvent.Time.Equal(now) {
				t.Errorf("Expected time %v, got %v", now, addEvent.Time)
			}
		}
	})

	// Test BidAcceptedEvent serialization
	t.Run("BidAcceptedEventSerialization", func(t *testing.T) {
		event := domain.BidAcceptedEvent{
			Time: now,
			Bid:  bid,
		}

		data, err := json.Marshal(event)
		if err != nil {
			t.Fatalf("Failed to marshal BidAcceptedEvent: %v", err)
		}

		var parsed interface{}
		err = json.Unmarshal(data, &parsed)
		if err != nil {
			t.Fatalf("Failed to unmarshal JSON: %v", err)
		}

		// Check that the JSON contains the expected type
		parsedMap := parsed.(map[string]interface{})
		if parsedMap["$type"] != "BidAccepted" {
			t.Errorf("Expected $type to be BidAccepted, got %v", parsedMap["$type"])
		}

		// Parse back to event
		parsedEvent, err := domain.UnmarshalEvent(data)
		if err != nil {
			t.Fatalf("Failed to unmarshal BidAcceptedEvent: %v", err)
		}

		// Check that we got the right type
		if bidEvent, ok := parsedEvent.(domain.BidAcceptedEvent); !ok {
			t.Errorf("Expected BidAcceptedEvent, got %T", parsedEvent)
		} else {
			// Check a few key fields
			if bidEvent.Bid.ForAuction != auctionId {
				t.Errorf("Expected auction ID %v, got %v", auctionId, bidEvent.Bid.ForAuction)
			}
			if !bidEvent.Time.Equal(now) {
				t.Errorf("Expected time %v, got %v", now, bidEvent.Time)
			}
		}
	})
}
