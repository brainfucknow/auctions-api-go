package web

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"auction-site-go/internal/domain"
)

// getAuctions returns all auctions
func getAuctions(state *AppState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		repo := state.GetRepository()
		auctions := domain.GetAuctions(repo)

		// Convert to AuctionListItem
		auctionItems := make([]AuctionListItem, len(auctions))
		for i, auction := range auctions {
			auctionItems[i] = AuctionListItem{
				ID:       auction.ID,
				StartsAt: auction.StartsAt,
				Title:    auction.Title,
				Expiry:   auction.Expiry,
				Currency: auction.Currency,
			}
		}

		respondJSON(w, http.StatusOK, auctionItems)
	}
}

// getAuction returns a specific auction
func getAuction(state *AppState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Parse auction ID from path
		vars := mux.Vars(r)
		idStr := vars["id"]
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, "Invalid auction ID")
			return
		}

		// Get auction from repository
		repo := state.GetRepository()
		entry, ok := repo[domain.AuctionId(id)]
		if !ok {
			respondError(w, http.StatusNotFound, "Auction not found")
			return
		}

		auction := entry.Auction
		auctionState := entry.State

		// Get bids
		bids := auctionState.GetBids()
		bidResponses := make([]AuctionBidResponse, len(bids))
		for i, bid := range bids {
			bidResponses[i] = AuctionBidResponse{
				Amount: bid.Amount,
				Bidder: bid.Bidder,
			}
		}

		// Get winner information
		var winner *domain.UserId
		var winnerPrice *int64
		if amount, userId, found := auctionState.TryGetAmountAndWinner(); found {
			winner = &userId
			winnerPrice = &amount
		}

		// Create response
		response := AuctionResponse{
			ID:          auction.ID,
			StartsAt:    auction.StartsAt,
			Title:       auction.Title,
			Expiry:      auction.Expiry,
			Currency:    auction.Currency,
			Bids:        bidResponses,
			Winner:      winner,
			WinnerPrice: winnerPrice,
		}

		respondJSON(w, http.StatusOK, response)
	}
}

// createAuction creates a new auction
func createAuction(state *AppState, onCommand func(domain.Command) error, onEvent func(domain.Event) error, getCurrentTime func() time.Time) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Parse request body
		var req AddAuctionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondError(w, http.StatusBadRequest, "Invalid request body")
			return
		}

		// Extract user from JWT
		user, err := extractUserFromRequest(r)
		if err != nil {
			respondError(w, http.StatusUnauthorized, "Unauthorized")
			return
		}

		// Create auction
		var auctionType domain.AuctionType
		if req.Type.Type != 0 {
			auctionType = req.Type
		} else {
			// Default to English auction
			options := domain.DefaultTimedAscendingOptions()
			auctionType = domain.NewTimedAscendingType(options)
		}

		auction := domain.Auction{
			ID:       req.ID,
			StartsAt: req.StartsAt,
			Title:    req.Title,
			Expiry:   req.EndsAt,
			Seller:   user,
			Type:     auctionType,
			Currency: req.Currency,
		}

		// Create command
		cmd := domain.AddAuctionCommand{
			Time:    getCurrentTime(),
			Auction: auction,
		}

		if err := onCommand(cmd); err != nil {
			log.Printf("Failed to observe command: %v", err)
			respondError(w, http.StatusInternalServerError, "Internal server error")
			return
		}

		// Handle command
		repo := state.GetRepository()
		event, newRepo, err := domain.Handle(cmd, repo)
		if err != nil {
			var domainErr domain.DomainError
			ok := false
			if domainErr, ok = err.(domain.DomainError); ok {
				if domainErr.Type == domain.ErrorAuctionAlreadyExists {
					respondError(w, http.StatusConflict, err.Error())
					return
				}
			}
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}

		// Update repository
		state.UpdateRepository(newRepo)

		// Call event handler
		if err := onEvent(event); err != nil {
			log.Printf("Failed to observe event: %v", err)
			respondError(w, http.StatusInternalServerError, "Internal server error")
			return
		}

		// Return the event
		respondJSON(w, http.StatusOK, event)
	}
}

// placeBid places a bid on an auction
func placeBid(state *AppState, onCommand func(domain.Command) error, onEvent func(domain.Event) error, getCurrentTime func() time.Time) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Parse auction ID from path
		vars := mux.Vars(r)
		idStr := vars["id"]
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, "Invalid auction ID")
			return
		}

		// Parse request body
		var req BidRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondError(w, http.StatusBadRequest, "Invalid request body")
			return
		}

		// Extract user from JWT
		user, err := extractUserFromRequest(r)
		if err != nil {
			respondError(w, http.StatusUnauthorized, "Unauthorized")
			return
		}

		// Create bid
		bid := domain.Bid{
			ForAuction: domain.AuctionId(id),
			Bidder:     user,
			At:         getCurrentTime(),
			Amount:     req.Amount,
		}

		// Create command
		cmd := domain.PlaceBidCommand{
			Time: getCurrentTime(),
			Bid:  bid,
		}

		if err := onCommand(cmd); err != nil {
			log.Printf("Failed to observe command: %v", err)
			respondError(w, http.StatusInternalServerError, "Internal server error")
			return
		}

		// Get auction from repository
		repo := state.GetRepository()
		_, ok := repo[domain.AuctionId(id)]
		if !ok {
			respondError(w, http.StatusNotFound, "Auction not found")
			return
		}

		// Handle command
		event, newRepo, err := domain.Handle(cmd, repo)
		if err != nil {
			var domainErr domain.DomainError
			ok := false
			if domainErr, ok = err.(domain.DomainError); ok {
				if domainErr.Type == domain.ErrorUnknownAuction {
					respondError(w, http.StatusNotFound, "Auction not found")
					return
				}
			}
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}

		// Update repository
		state.UpdateRepository(newRepo)

		// Call event handler
		if err := onEvent(event); err != nil {
			log.Printf("Failed to observe event: %v", err)
			respondError(w, http.StatusInternalServerError, "Internal server error")
			return
		}

		// Return the event
		respondJSON(w, http.StatusOK, event)
	}
}

// extractUserFromRequest extracts a user from an HTTP request
func extractUserFromRequest(r *http.Request) (domain.User, error) {
	authHeader := r.Header.Get("x-jwt-payload")
	if authHeader == "" {
		return domain.User{}, errors.New("missing authentication header")
	}

	// In the test, trim any whitespace
	authHeader = strings.TrimSpace(authHeader)

	return DecodeJwtUser(authHeader)
}

// respondJSON responds with a JSON payload
func respondJSON(w http.ResponseWriter, status int, payload interface{}) {
	response, err := json.Marshal(payload)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(response)
}

// respondError responds with an error message
func respondError(w http.ResponseWriter, status int, message string) {
	respondJSON(w, status, ApiError{Message: message})
}
