package web_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"auction-site-go/internal/domain"
	"auction-site-go/internal/web"
)

// TestAPI tests the HTTP API endpoints
func TestAPI(t *testing.T) {
	// Fixed time for tests
	fixedTime, _ := time.Parse(time.RFC3339, "2018-08-04T00:00:00Z")
	getCurrentTime := func() time.Time {
		return fixedTime
	}

	// Command handler that just records events
	var recordedCommands []domain.Command
	onCommand := func(command domain.Command) error {
		recordedCommands = append(recordedCommands, command)
		return nil
	}

	// Event handler that just records events
	var recordedEvents []domain.Event
	onEvent := func(event domain.Event) error {
		recordedEvents = append(recordedEvents, event)
		return nil
	}

	// Create app with empty repository
	app := web.NewApp(domain.Repository{}, onCommand, onEvent, getCurrentTime)

	// Define JWT headers
	sellerJWT := "eyJzdWIiOiJhMSIsICJuYW1lIjoiVGVzdCIsICJ1X3R5cCI6IjAifQo="
	buyerJWT := "eyJzdWIiOiJhMiIsICJuYW1lIjoiQnV5ZXIiLCAidV90eXAiOiIwIn0K"

	// Define test auction request
	auctionReq := `{
		"id": 1,
		"startsAt": "2018-01-01T10:00:00.000Z",
		"endsAt": "2019-01-01T10:00:00.000Z",
		"title": "First auction",
		"currency": "VAC"
	}`

	// Test adding an auction
	t.Run("AddAuction", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "/auctions", bytes.NewBufferString(auctionReq))
		req.Header.Set("x-jwt-payload", sellerJWT)
		req.Header.Set("Content-Type", "application/json")

		// Reset recorded events
		recordedEvents = nil

		// Execute request
		rr := httptest.NewRecorder()
		app.Router.ServeHTTP(rr, req)

		// Check response
		if status := rr.Code; status != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
			t.Logf("Response body: %s", rr.Body.String())
		}

		// Check that an event was created
		if len(recordedEvents) != 1 {
			t.Fatalf("expected 1 event, got %d", len(recordedEvents))
		}

		// Check event type
		auctionAddedEvent, ok := recordedEvents[0].(domain.AuctionAddedEvent)
		if !ok {
			t.Fatalf("expected AuctionAddedEvent, got %T", recordedEvents[0])
		}

		// Check event data
		if auctionAddedEvent.Auction.ID != 1 {
			t.Errorf("expected auction ID 1, got %d", auctionAddedEvent.Auction.ID)
		}
	})

	// Test can't add same auction twice
	t.Run("CantAddSameAuctionTwice", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "/auctions", bytes.NewBufferString(auctionReq))
		req.Header.Set("x-jwt-payload", sellerJWT)
		req.Header.Set("Content-Type", "application/json")

		// Execute request
		rr := httptest.NewRecorder()
		app.Router.ServeHTTP(rr, req)

		// Check response - should be conflict (409)
		if status := rr.Code; status != http.StatusConflict {
			t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusConflict)
		}
	})

	// Test get auctions
	t.Run("GetAuctions", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/auctions", nil)

		// Execute request
		rr := httptest.NewRecorder()
		app.Router.ServeHTTP(rr, req)

		// Check response
		if status := rr.Code; status != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
		}

		// Parse response
		var auctions []web.AuctionListItem
		err := json.Unmarshal(rr.Body.Bytes(), &auctions)
		if err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		// Check that we have the auction
		if len(auctions) != 1 {
			t.Fatalf("expected 1 auction, got %d", len(auctions))
		}

		if auctions[0].ID != 1 {
			t.Errorf("expected auction ID 1, got %d", auctions[0].ID)
		}
	})

	// Test get auction
	t.Run("GetAuction", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/auctions/1", nil)

		// Execute request
		rr := httptest.NewRecorder()
		app.Router.ServeHTTP(rr, req)

		// Check response
		if status := rr.Code; status != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
		}

		// Parse response
		var auction web.AuctionResponse
		err := json.Unmarshal(rr.Body.Bytes(), &auction)
		if err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		// Check auction data
		if auction.ID != 1 {
			t.Errorf("expected auction ID 1, got %d", auction.ID)
		}

		// Initially there should be no bids
		if len(auction.Bids) != 0 {
			t.Errorf("expected 0 bids, got %d", len(auction.Bids))
		}

		// No winner yet
		if auction.Winner != nil {
			t.Errorf("expected no winner, got %s", *auction.Winner)
		}
	})

	// Test place bid
	t.Run("PlaceBid", func(t *testing.T) {
		bidReq := `{"amount": 11}`
		req, _ := http.NewRequest("POST", "/auctions/1/bids", bytes.NewBufferString(bidReq))
		req.Header.Set("x-jwt-payload", buyerJWT)
		req.Header.Set("Content-Type", "application/json")

		// Reset recorded events
		recordedEvents = nil

		// Execute request
		rr := httptest.NewRecorder()
		app.Router.ServeHTTP(rr, req)

		// Check response
		if status := rr.Code; status != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
			t.Logf("Response body: %s", rr.Body.String())
		}

		// Check that an event was created
		if len(recordedEvents) != 1 {
			t.Fatalf("expected 1 event, got %d", len(recordedEvents))
		}

		// Check event type
		bidAcceptedEvent, ok := recordedEvents[0].(domain.BidAcceptedEvent)
		if !ok {
			t.Fatalf("expected BidAcceptedEvent, got %T", recordedEvents[0])
		}

		// Check event data
		if bidAcceptedEvent.Bid.ForAuction != 1 {
			t.Errorf("expected auction ID 1, got %d", bidAcceptedEvent.Bid.ForAuction)
		}

		if bidAcceptedEvent.Bid.Amount != 11 {
			t.Errorf("expected bid amount 11, got %d", bidAcceptedEvent.Bid.Amount)
		}
	})

	// Test get auction with bids
	t.Run("GetAuctionWithBids", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/auctions/1", nil)

		// Execute request
		rr := httptest.NewRecorder()
		app.Router.ServeHTTP(rr, req)

		// Check response
		if status := rr.Code; status != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
		}

		// Parse response
		var auction web.AuctionResponse
		err := json.Unmarshal(rr.Body.Bytes(), &auction)
		if err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		// Now there should be 1 bid
		if len(auction.Bids) != 1 {
			t.Errorf("expected 1 bid, got %d", len(auction.Bids))
		} else {
			if auction.Bids[0].Amount != 11 {
				t.Errorf("expected bid amount 11, got %d", auction.Bids[0].Amount)
			}

			// Check bidder
			if auction.Bids[0].Bidder.ID != "a2" {
				t.Errorf("expected bidder a2, got %s", auction.Bids[0].Bidder.ID)
			}
		}
	})

	// Test bid on non-existent auction
	t.Run("BidOnNonExistentAuction", func(t *testing.T) {
		bidReq := `{"amount": 10}`
		req, _ := http.NewRequest("POST", "/auctions/999/bids", bytes.NewBufferString(bidReq))
		req.Header.Set("x-jwt-payload", buyerJWT)
		req.Header.Set("Content-Type", "application/json")

		// Execute request
		rr := httptest.NewRecorder()
		app.Router.ServeHTTP(rr, req)

		// Check response - should be not found (404)
		if status := rr.Code; status != http.StatusNotFound {
			t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusNotFound)
		}
	})

	// Test seller cannot bid on own auction
	t.Run("SellerCannotBidOnOwnAuction", func(t *testing.T) {
		bidReq := `{"amount": 12}`
		req, _ := http.NewRequest("POST", "/auctions/1/bids", bytes.NewBufferString(bidReq))
		req.Header.Set("x-jwt-payload", sellerJWT)
		req.Header.Set("Content-Type", "application/json")

		// Execute request
		rr := httptest.NewRecorder()
		app.Router.ServeHTTP(rr, req)

		// Check response - should be bad request (400)
		if status := rr.Code; status != http.StatusBadRequest {
			t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusBadRequest)
		}

		// Response should contain an error about seller not being able to bid
		if !bytes.Contains(rr.Body.Bytes(), []byte("SellerCannotPlaceBids")) {
			t.Errorf("expected error about seller not being able to bid, got: %s", rr.Body.String())
		}
	})

	// Test unauthorized access
	t.Run("UnauthorizedAccess", func(t *testing.T) {
		// Try to create an auction without JWT
		req, _ := http.NewRequest("POST", "/auctions", bytes.NewBufferString(auctionReq))
		req.Header.Set("Content-Type", "application/json")

		// Execute request
		rr := httptest.NewRecorder()
		app.Router.ServeHTTP(rr, req)

		// Check response - should be unauthorized (401)
		if status := rr.Code; status != http.StatusUnauthorized {
			t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusUnauthorized)
		}
	})
}
