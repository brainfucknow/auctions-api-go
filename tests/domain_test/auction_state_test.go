package domain_test

import (
	"testing"
	"time"

	"auction-site-go/internal/domain"
)

// Sample data for tests
var (
	sampleAuctionId = domain.AuctionId(1)
	sampleTitle     = "auction"
	sampleStartsAt  = mustParseTime("2016-01-01T08:28:00.607875Z")
	sampleEndsAt    = mustParseTime("2016-02-01T08:28:00.607875Z")
	sampleBidTime   = mustParseTime("2016-02-01T07:28:00.607875Z")
	sampleSeller    = domain.NewBuyerOrSeller("Sample_Seller", "Seller")
	sampleBuyer     = domain.NewBuyerOrSeller("Sample_Buyer", "Buyer")
	buyer1          = domain.NewBuyerOrSeller("Buyer_1", "Buyer 1")
	buyer2          = domain.NewBuyerOrSeller("Buyer_2", "Buyer 2")
	buyer3          = domain.NewBuyerOrSeller("Buyer_3", "Buyer 3")
	bidAmount1      = domain.Amount{Currency: domain.SEK, Value: 10}
	bidAmount2      = domain.Amount{Currency: domain.SEK, Value: 12}
)

// Helper function to parse time
func mustParseTime(timeStr string) time.Time {
	t, err := time.Parse(time.RFC3339, timeStr)
	if err != nil {
		panic(err)
	}
	return t
}

// Create sample auction of specified type
func sampleAuctionOfType(auctionType domain.AuctionType) domain.Auction {
	return domain.Auction{
		ID:       sampleAuctionId,
		Title:    sampleTitle,
		StartsAt: sampleStartsAt,
		Expiry:   sampleEndsAt,
		Seller:   sampleSeller,
		Currency: domain.SEK,
		Type:     auctionType,
	}
}

// Create sample bids
func createBid1() domain.Bid {
	return domain.Bid{
		ForAuction: sampleAuctionId,
		Bidder:     buyer1,
		At:         sampleStartsAt.Add(time.Second),
		Amount:     bidAmount1,
	}
}

func createBid2() domain.Bid {
	return domain.Bid{
		ForAuction: sampleAuctionId,
		Bidder:     buyer2,
		At:         sampleStartsAt.Add(2 * time.Second),
		Amount:     bidAmount2,
	}
}

func createBidLessThan2() domain.Bid {
	return domain.Bid{
		ForAuction: sampleAuctionId,
		Bidder:     buyer3,
		At:         sampleStartsAt.Add(3 * time.Second),
		Amount:     domain.Amount{Currency: domain.SEK, Value: 11},
	}
}

// Test fixture for auction state tests
type auctionStateTestFixture struct {
	auction domain.Auction
	state   domain.State
	bid1    domain.Bid
	bid2    domain.Bid
}

// Create a new test fixture for the given auction type
func newAuctionStateTestFixture(auctionType domain.AuctionType) auctionStateTestFixture {
	auction := sampleAuctionOfType(auctionType)
	return auctionStateTestFixture{
		auction: auction,
		state:   auction.CreateEmptyState(),
		bid1:    createBid1(),
		bid2:    createBid2(),
	}
}

// Common state increment tests
func testStateIncrement(t *testing.T, state domain.State) {
	t.Run("IncrementTwice", func(t *testing.T) {
		s := state.Increment(sampleBidTime)
		s2 := s.Increment(sampleBidTime)

		// Check that incrementing twice with the same time results in the same state
		// Go doesn't have a built-in way to compare struct equality for interfaces
		// Here we can check if both states have the same "ended" status
		if s.HasEnded() != s2.HasEnded() {
			t.Errorf("Expected ended status to be the same, got %v and %v", s.HasEnded(), s2.HasEnded())
		}
	})

	t.Run("WontEndJustAfterStart", func(t *testing.T) {
		s := state.Increment(sampleStartsAt.Add(time.Second))
		if s.HasEnded() {
			t.Errorf("Expected auction not to have ended just after start")
		}
	})

	t.Run("WontEndJustBeforeEnd", func(t *testing.T) {
		s := state.Increment(sampleEndsAt.Add(-time.Second))
		if s.HasEnded() {
			t.Errorf("Expected auction not to have ended just before end")
		}
	})

	t.Run("WontEndJustBeforeStart", func(t *testing.T) {
		s := state.Increment(sampleStartsAt.Add(-time.Second))
		if s.HasEnded() {
			t.Errorf("Expected auction not to have ended just before start")
		}
	})

	t.Run("WillHaveEndedJustAfterEnd", func(t *testing.T) {
		s := state.Increment(sampleEndsAt.Add(time.Second))
		if !s.HasEnded() {
			t.Errorf("Expected auction to have ended just after end")
		}
	})
}

// Common tests for sealed bid auctions (both Blind and Vickrey)
func testSealedBidAuction(t *testing.T, fixture auctionStateTestFixture, expectedWinningAmount domain.Amount) {
	t.Run("CanAddBidToEmptyState", func(t *testing.T) {
		stateWith1Bid, err := fixture.state.AddBid(fixture.bid1)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		// Check that the bid was added
		bids := stateWith1Bid.GetBids()
		if len(bids) != 1 {
			t.Errorf("Expected 1 bid, got %d", len(bids))
		}
	})

	t.Run("CanAddSecondBid", func(t *testing.T) {
		stateWith1Bid, _ := fixture.state.AddBid(fixture.bid1)
		stateWith2Bids, err := stateWith1Bid.AddBid(fixture.bid2)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		// Check that the second bid was added
		bids := stateWith2Bids.GetBids()
		if len(bids) != 2 {
			t.Errorf("Expected 2 bids, got %d", len(bids))
		}
	})

	t.Run("CanEnd", func(t *testing.T) {
		stateWith1Bid, _ := fixture.state.AddBid(fixture.bid1)
		stateWith2Bids, _ := stateWith1Bid.AddBid(fixture.bid2)
		stateEndedAfterTwoBids := stateWith2Bids.Increment(sampleEndsAt)

		// Check that the auction has ended
		if !stateEndedAfterTwoBids.HasEnded() {
			t.Errorf("Expected auction to have ended")
		}

		// No more bids can be added after end
		_, err := stateEndedAfterTwoBids.AddBid(createBidLessThan2())
		if err == nil {
			t.Errorf("Expected error when adding bid to ended auction")
		}
	})

	t.Run("CanGetWinnerAndPriceFromEndedAuction", func(t *testing.T) {
		stateWith1Bid, _ := fixture.state.AddBid(fixture.bid1)
		stateWith2Bids, _ := stateWith1Bid.AddBid(fixture.bid2)
		stateEndedAfterTwoBids := stateWith2Bids.Increment(sampleEndsAt)

		amount, winner, found := stateEndedAfterTwoBids.TryGetAmountAndWinner()
		if !found {
			t.Errorf("Expected to find winner and price")
		}
		if amount != expectedWinningAmount {
			t.Errorf("Expected winning amount to be %v, got %v", expectedWinningAmount, amount)
		}
		if winner != buyer2.ID {
			t.Errorf("Expected winner to be %s, got %s", buyer2.ID, winner)
		}
	})

	// Run common increment tests
	testStateIncrement(t, fixture.state)
}

// Test blind auction
func TestBlindAuctionState(t *testing.T) {
	fixture := newAuctionStateTestFixture(domain.NewSingleSealedBidType(domain.Blind))
	// In Blind auctions, the highest bidder wins and pays their bid
	testSealedBidAuction(t, fixture, bidAmount2)
}

// Test Vickrey auction
func TestVickreyAuctionState(t *testing.T) {
	fixture := newAuctionStateTestFixture(domain.NewSingleSealedBidType(domain.Vickrey))
	// In Vickrey auctions, the highest bidder wins but pays the second-highest bid
	testSealedBidAuction(t, fixture, bidAmount1)
}

// Test timed ascending (English) auction
func TestTimedAscendingAuctionState(t *testing.T) {
	options := domain.DefaultTimedAscendingOptions(domain.SEK)
	fixture := newAuctionStateTestFixture(domain.NewTimedAscendingType(options))
	bidLessThan2 := createBidLessThan2()

	// Helper function to get an active state (past the AwaitingStart state)
	getActiveState := func() domain.State {
		return fixture.state.Increment(sampleStartsAt.Add(time.Second))
	}

	t.Run("CanAddBidToEmptyState", func(t *testing.T) {
		// First we need to get out of the AwaitingStart state
		activeState := getActiveState()

		stateWith1Bid, err := activeState.AddBid(fixture.bid1)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		// Check that the bid was added
		bids := stateWith1Bid.GetBids()
		if len(bids) != 1 {
			t.Errorf("Expected 1 bid, got %d", len(bids))
		}
	})

	t.Run("CanAddSecondBid", func(t *testing.T) {
		activeState := getActiveState()
		stateWith1Bid, _ := activeState.AddBid(fixture.bid1)
		stateWith2Bids, err := stateWith1Bid.AddBid(fixture.bid2)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		// Check that the second bid was added
		bids := stateWith2Bids.GetBids()
		if len(bids) != 2 {
			t.Errorf("Expected 2 bids, got %d", len(bids))
		}
	})

	t.Run("CanEnd", func(t *testing.T) {
		endedState := fixture.state.Increment(sampleEndsAt.Add(time.Second))

		// Check that the auction has ended
		if !endedState.HasEnded() {
			t.Errorf("Expected auction to have ended")
		}
	})

	t.Run("EndedWithTwoBids", func(t *testing.T) {
		activeState := getActiveState()
		stateWith1Bid, _ := activeState.AddBid(fixture.bid1)
		stateWith2Bids, _ := stateWith1Bid.AddBid(fixture.bid2)
		stateEndedAfterTwoBids := stateWith2Bids.Increment(sampleEndsAt.Add(time.Second))

		// Check that the auction has ended
		if !stateEndedAfterTwoBids.HasEnded() {
			t.Errorf("Expected auction to have ended")
		}

		// Check that bids are preserved
		bids := stateEndedAfterTwoBids.GetBids()
		if len(bids) != 2 {
			t.Errorf("Expected 2 bids, got %d", len(bids))
		}
	})

	t.Run("CannotBidAfterAuctionHasEnded", func(t *testing.T) {
		activeState := getActiveState()
		stateWith1Bid, _ := activeState.AddBid(fixture.bid1)
		stateEndedAfter1Bid := stateWith1Bid.Increment(sampleEndsAt.Add(time.Second))

		_, err := stateEndedAfter1Bid.AddBid(fixture.bid2)
		if err == nil {
			t.Errorf("Expected error when bidding on ended auction")
		}

		// Check for the specific error type
		if domainErr, ok := err.(domain.DomainError); !ok || domainErr.Type != domain.ErrorAuctionHasEnded {
			t.Errorf("Expected AuctionHasEnded error, got %v", err)
		}
	})

	t.Run("CanGetWinnerAndPriceFromEndedAuction", func(t *testing.T) {
		activeState := getActiveState()
		stateWith1Bid, _ := activeState.AddBid(fixture.bid1)
		stateWith2Bids, _ := stateWith1Bid.AddBid(fixture.bid2)
		stateEndedAfterTwoBids := stateWith2Bids.Increment(sampleEndsAt.Add(time.Second))

		amount, winner, found := stateEndedAfterTwoBids.TryGetAmountAndWinner()
		if !found {
			t.Errorf("Expected to find winner and price")
		}

		// In TimedAscending auctions, the highest bidder wins and pays their bid
		if amount != bidAmount2 {
			t.Errorf("Expected winning amount to be %v, got %v", bidAmount2, amount)
		}
		if winner != buyer2.ID {
			t.Errorf("Expected winner to be %s, got %s", buyer2.ID, winner)
		}
	})

	t.Run("CannotPlaceBidLowerThanHighestBid", func(t *testing.T) {
		activeState := getActiveState()
		stateWith1Bid, _ := activeState.AddBid(fixture.bid2) // Higher bid first

		_, err := stateWith1Bid.AddBid(bidLessThan2) // Lower bid after
		if err == nil {
			t.Errorf("Expected error when bidding lower than highest bid")
		}

		// Check for the specific error type
		if domainErr, ok := err.(domain.DomainError); !ok || domainErr.Type != domain.ErrorMustPlaceBidOverHighest {
			t.Errorf("Expected MustPlaceBidOverHighestBid error, got %v", err)
		}
	})

	// Test reservation price
	t.Run("ReservePriceWorks", func(t *testing.T) {
		testReservePriceForTimedAscending(t)
	})

	// Test minimum raise
	t.Run("MinimumRaiseWorks", func(t *testing.T) {
		testMinimumRaiseForTimedAscending(t)
	})

	// Test time frame extension
	t.Run("TimeFrameWorks", func(t *testing.T) {
		testTimeFrameForTimedAscending(t)
	})

	// Run common increment tests
	testStateIncrement(t, fixture.state)
}

// Helper function to test reserve price for timed ascending auctions
func testReservePriceForTimedAscending(t *testing.T) {
	// Create an auction with a reserve price
	reserveOptions := domain.TimedAscendingOptions{
		ReservePrice: domain.Amount{Currency: domain.SEK, Value: 15},
		MinRaise:     domain.Amount{Currency: domain.SEK, Value: 0},
		TimeFrame:    0,
	}

	reserveAuction := sampleAuctionOfType(domain.NewTimedAscendingType(reserveOptions))
	reserveState := reserveAuction.CreateEmptyState()

	// Activate auction
	activeState := reserveState.Increment(sampleStartsAt.Add(time.Second))

	// Add a bid below reserve price
	stateWith1Bid, _ := activeState.AddBid(createBid2()) // bid2 has value 12, below reserve of 15
	stateEndedAfter1Bid := stateWith1Bid.Increment(sampleEndsAt.Add(time.Second))

	// Should not have a winner since bid is below reserve
	_, _, found := stateEndedAfter1Bid.TryGetAmountAndWinner()
	if found {
		t.Errorf("Expected no winner when highest bid is below reserve price")
	}

	// Now add a bid above reserve price
	highBid := domain.Bid{
		ForAuction: sampleAuctionId,
		Bidder:     buyer3,
		At:         sampleStartsAt.Add(3 * time.Second),
		Amount:     domain.Amount{Currency: domain.SEK, Value: 20},
	}

	// Start with a fresh state
	activeState = reserveState.Increment(sampleStartsAt.Add(time.Second))
	stateWithHighBid, _ := activeState.AddBid(highBid)
	stateEndedAfterHighBid := stateWithHighBid.Increment(sampleEndsAt.Add(time.Second))

	// Should have a winner since bid is above reserve
	amount, winner, found := stateEndedAfterHighBid.TryGetAmountAndWinner()
	if !found {
		t.Errorf("Expected to find winner when highest bid is above reserve price")
	}

	if amount.Value != 20 {
		t.Errorf("Expected winning amount to be 20, got %v", amount.Value)
	}

	if winner != buyer3.ID {
		t.Errorf("Expected winner to be %s, got %s", buyer3.ID, winner)
	}
}

// Helper function to test minimum raise for timed ascending auctions
func testMinimumRaiseForTimedAscending(t *testing.T) {
	// Create an auction with a minimum raise requirement
	minRaiseOptions := domain.TimedAscendingOptions{
		ReservePrice: domain.Amount{Currency: domain.SEK, Value: 0},
		MinRaise:     domain.Amount{Currency: domain.SEK, Value: 5},
		TimeFrame:    0,
	}

	minRaiseAuction := sampleAuctionOfType(domain.NewTimedAscendingType(minRaiseOptions))
	minRaiseState := minRaiseAuction.CreateEmptyState()

	// Activate auction
	activeState := minRaiseState.Increment(sampleStartsAt.Add(time.Second))

	// Add first bid
	stateWith1Bid, _ := activeState.AddBid(createBid1()) // bid1 has value 10

	// Try to add a second bid that doesn't meet minimum raise
	smallRaiseBid := domain.Bid{
		ForAuction: sampleAuctionId,
		Bidder:     buyer2,
		At:         sampleStartsAt.Add(2 * time.Second),
		Amount:     domain.Amount{Currency: domain.SEK, Value: 14}, // Only 4 more than bid1
	}

	_, err := stateWith1Bid.AddBid(smallRaiseBid)
	if err == nil {
		t.Errorf("Expected error when bid doesn't meet minimum raise")
	}

	// Now try with a bid that does meet the minimum raise
	goodRaiseBid := domain.Bid{
		ForAuction: sampleAuctionId,
		Bidder:     buyer2,
		At:         sampleStartsAt.Add(2 * time.Second),
		Amount:     domain.Amount{Currency: domain.SEK, Value: 15}, // 5 more than bid1
	}

	stateWith2Bids, err := stateWith1Bid.AddBid(goodRaiseBid)
	if err != nil {
		t.Errorf("Expected no error when bid meets minimum raise, got %v", err)
	}

	bids := stateWith2Bids.GetBids()
	if len(bids) != 2 {
		t.Errorf("Expected 2 bids, got %d", len(bids))
	}

	if bids[0].Amount.Value != 15 {
		t.Errorf("Expected highest bid to be 15, got %v", bids[0].Amount.Value)
	}
}

// Helper function to test time frame for timed ascending auctions
func testTimeFrameForTimedAscending(t *testing.T) {
	// Create an auction with a time frame
	timeFrameOptions := domain.TimedAscendingOptions{
		ReservePrice: domain.Amount{Currency: domain.SEK, Value: 0},
		MinRaise:     domain.Amount{Currency: domain.SEK, Value: 0},
		TimeFrame:    10 * time.Minute,
	}

	timeFrameAuction := sampleAuctionOfType(domain.NewTimedAscendingType(timeFrameOptions))
	timeFrameState := timeFrameAuction.CreateEmptyState()

	// Activate auction
	activeState := timeFrameState.Increment(sampleStartsAt.Add(time.Second))

	// Add a bid 5 minutes before the end
	bidTime := sampleEndsAt.Add(-5 * time.Minute)
	lateBid := domain.Bid{
		ForAuction: sampleAuctionId,
		Bidder:     buyer1,
		At:         bidTime,
		Amount:     domain.Amount{Currency: domain.SEK, Value: 10},
	}

	stateWithLateBid, _ := activeState.AddBid(lateBid)

	// The auction should not have ended right at the original end time
	stateAtOriginalEnd := stateWithLateBid.Increment(sampleEndsAt)
	if stateAtOriginalEnd.HasEnded() {
		t.Errorf("Expected auction not to have ended at original end time after a late bid")
	}

	// But it should end after the time frame has elapsed from the last bid
	stateAfterTimeFrame := stateWithLateBid.Increment(bidTime.Add(timeFrameOptions.TimeFrame).Add(time.Second))
	if !stateAfterTimeFrame.HasEnded() {
		t.Errorf("Expected auction to have ended after time frame elapsed from last bid")
	}
}

// Test command handling
func TestCommandHandling(t *testing.T) {
	// Create an auction
	options := domain.DefaultTimedAscendingOptions(domain.SEK)
	auction := sampleAuctionOfType(domain.NewTimedAscendingType(options))
	now := time.Now()

	// Create an empty repository
	repo := domain.Repository{}

	// Test AddAuctionCommand
	t.Run("AddAuctionCommand", func(t *testing.T) {
		cmd := domain.AddAuctionCommand{
			Time:    now,
			Auction: auction,
		}

		event, newRepo, err := domain.Handle(cmd, repo)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		// Check the event
		auctionAddedEvent, ok := event.(domain.AuctionAddedEvent)
		if !ok {
			t.Errorf("Expected AuctionAddedEvent, got %T", event)
		}

		if auctionAddedEvent.Auction.ID != auction.ID {
			t.Errorf("Expected auction ID %v, got %v", auction.ID, auctionAddedEvent.Auction.ID)
		}

		// Check the repo
		if len(newRepo) != 1 {
			t.Errorf("Expected 1 auction in repo, got %d", len(newRepo))
		}

		// Update repo for next test
		repo = newRepo
	})

	// Test adding duplicate auction
	t.Run("AddDuplicateAuctionCommand", func(t *testing.T) {
		cmd := domain.AddAuctionCommand{
			Time:    now,
			Auction: auction,
		}

		_, _, err := domain.Handle(cmd, repo)
		if err == nil {
			t.Errorf("Expected error when adding duplicate auction")
		}

		// Check for the specific error type
		if domainErr, ok := err.(domain.DomainError); !ok || domainErr.Type != domain.ErrorAuctionAlreadyExists {
			t.Errorf("Expected AuctionAlreadyExists error, got %v", err)
		}
	})

	// Test PlaceBidCommand
	t.Run("PlaceBidCommand", func(t *testing.T) {
		// First advance the time to start the auction
		auctionEntry := repo[auction.ID]
		activeState := auctionEntry.State.Increment(sampleStartsAt.Add(time.Second))
		repo[auction.ID] = struct {
			Auction domain.Auction
			State   domain.State
		}{
			Auction: auctionEntry.Auction,
			State:   activeState,
		}

		// Create a bid
		bid := domain.Bid{
			ForAuction: auction.ID,
			Bidder:     buyer1,
			At:         sampleStartsAt.Add(time.Second),
			Amount:     domain.Amount{Currency: domain.SEK, Value: 10},
		}

		cmd := domain.PlaceBidCommand{
			Time: now,
			Bid:  bid,
		}

		event, newRepo, err := domain.Handle(cmd, repo)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		// Check the event
		bidAcceptedEvent, ok := event.(domain.BidAcceptedEvent)
		if !ok {
			t.Errorf("Expected BidAcceptedEvent, got %T", event)
		}

		if bidAcceptedEvent.Bid.ForAuction != auction.ID {
			t.Errorf("Expected auction ID %v, got %v", auction.ID, bidAcceptedEvent.Bid.ForAuction)
		}

		// Check the repo - should still have 1 auction
		if len(newRepo) != 1 {
			t.Errorf("Expected 1 auction in repo, got %d", len(newRepo))
		}

		// But now it should have a bid
		auctionEntry = newRepo[auction.ID]
		bids := auctionEntry.State.GetBids()
		if len(bids) != 1 {
			t.Errorf("Expected 1 bid, got %d", len(bids))
		}
	})
}
