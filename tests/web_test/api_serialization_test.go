package web_test

import (
	"encoding/json"
	"testing"
	"time"

	"auction-site-go/internal/domain"
	"auction-site-go/internal/web"
)

// TestAuctionDeserialization verifies that auction requests can be correctly deserialized
func TestAuctionDeserialization(t *testing.T) {
	// Create a sample JSON string
	jsonString := `{
		"id": 1,
		"startsAt": "2016-01-01T00:00:00.000Z",
		"endsAt": "2016-02-01T00:00:00.000Z",
		"title": "First auction"
	}`

	// Parse the JSON
	var req web.AddAuctionRequest
	err := json.Unmarshal([]byte(jsonString), &req)
	if err != nil {
		t.Fatalf("Failed to unmarshal auction request: %v", err)
	}

	// Check the parsed values
	expectedStartsAt, _ := time.Parse(time.RFC3339, "2016-01-01T00:00:00.000Z")
	expectedEndsAt, _ := time.Parse(time.RFC3339, "2016-02-01T00:00:00.000Z")

	if req.ID != 1 {
		t.Errorf("Expected ID to be 1, got %d", req.ID)
	}

	if !req.StartsAt.Equal(expectedStartsAt) {
		t.Errorf("Expected StartsAt to be %v, got %v", expectedStartsAt, req.StartsAt)
	}

	if !req.EndsAt.Equal(expectedEndsAt) {
		t.Errorf("Expected EndsAt to be %v, got %v", expectedEndsAt, req.EndsAt)
	}

	if req.Title != "First auction" {
		t.Errorf("Expected Title to be 'First auction', got '%s'", req.Title)
	}

	// Default currency should be VAC when not specified
	if req.Currency != domain.VAC {
		t.Errorf("Expected Currency to be VAC, got %s", req.Currency)
	}

	// Check default auction type
	// In the Go version, we'd typically set this in the handler when it's not provided
	// but for now we just check it's empty in the request
	var zeroValue domain.AuctionTypeEnum // Zero value of the enum
	if req.Type.Type != zeroValue {
		t.Errorf("Expected Type to be empty, got %v", req.Type.Type)
	}
}

// TestBidDeserialization verifies that bid requests can be correctly deserialized
func TestBidDeserialization(t *testing.T) {
	// Create a sample JSON string
	jsonString := `{ "amount": 10 }`

	// Parse the JSON
	var req web.BidRequest
	err := json.Unmarshal([]byte(jsonString), &req)
	if err != nil {
		t.Fatalf("Failed to unmarshal bid request: %v", err)
	}

	// Check the parsed values
	if req.Amount != 10 {
		t.Errorf("Expected Amount to be 10, got %d", req.Amount)
	}
}

// TestAmountSerialization verifies that Amount can be correctly serialized/deserialized
func TestAmountSerialization(t *testing.T) {
	// Create an Amount and serialize it
	amount := domain.Amount{Currency: domain.VAC, Value: 10}
	data, err := json.Marshal(amount)
	if err != nil {
		t.Fatalf("Failed to marshal amount: %v", err)
	}

	// Check the serialized string
	expected := `"VAC10"`
	if string(data) != expected {
		t.Errorf("Expected serialized amount to be %s, got %s", expected, string(data))
	}

	// Deserialize it back
	var parsedAmount domain.Amount
	err = json.Unmarshal(data, &parsedAmount)
	if err != nil {
		t.Fatalf("Failed to unmarshal amount: %v", err)
	}

	// Check the parsed values
	if parsedAmount.Currency != domain.VAC || parsedAmount.Value != 10 {
		t.Errorf("Expected Amount to be VAC10, got %s%d", parsedAmount.Currency, parsedAmount.Value)
	}
}

// TestUserSerialization verifies that User can be correctly serialized/deserialized
func TestUserSerialization(t *testing.T) {
	// Create a User and serialize it
	user := domain.NewBuyerOrSeller("a1", "Test")
	data, err := json.Marshal(user)
	if err != nil {
		t.Fatalf("Failed to marshal user: %v", err)
	}

	// Check the serialized string
	expected := `"BuyerOrSeller|a1|Test"`
	if string(data) != expected {
		t.Errorf("Expected serialized user to be %s, got %s", expected, string(data))
	}

	// Deserialize it back
	var parsedUser domain.User
	err = json.Unmarshal(data, &parsedUser)
	if err != nil {
		t.Fatalf("Failed to unmarshal user: %v", err)
	}

	// Check the parsed values
	if parsedUser.ID != "a1" || parsedUser.Name != "Test" || parsedUser.Type != "BuyerOrSeller" {
		t.Errorf("Expected User to be BuyerOrSeller|a1|Test, got %s|%s|%s",
			parsedUser.Type, parsedUser.ID, parsedUser.Name)
	}
}

// TestAuctionTypeSerialization verifies that AuctionType can be correctly serialized/deserialized
func TestAuctionTypeSerialization(t *testing.T) {
	// Create a serialized auction type string
	auctionTypeStr := `"Vickrey"`

	// Parse the string to AuctionType
	var parsedType domain.AuctionType
	err := json.Unmarshal([]byte(auctionTypeStr), &parsedType)
	if err != nil {
		t.Fatalf("Failed to unmarshal auction type: %v", err)
	}

	// Check the parsed values
	if parsedType.Type != domain.SingleSealedBid || parsedType.Options != "Vickrey" {
		t.Errorf("Expected AuctionType to be SingleSealedBid with options Vickrey, got %v with options %s",
			parsedType.Type, parsedType.Options)
	}

	// Now test a timed ascending auction type
	options := domain.DefaultTimedAscendingOptions()
	auctionType := domain.NewTimedAscendingType(options)

	// Marshal it
	data, err := json.Marshal(auctionType)
	if err != nil {
		t.Fatalf("Failed to marshal auction type: %v", err)
	}

	// The serialized type should contain "English"
	if string(data) != `"English|0|0|0"` {
		t.Errorf("Expected auction type to serialize as \"English|0|0|0\", got %s", string(data))
	}
}
