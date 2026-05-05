# Auction Site - Go Implementation

This project is a Go implementation of an auction site API that supports different types of auctions:

1. **Timed Ascending (English)** auctions - where bidders openly bid against each other, and the highest bidder wins
2. **Single Sealed Bid** auctions:
   - **Blind** - highest bidder pays their bid amount
   - **Vickrey** - highest bidder pays the second-highest bid amount

## Features

- RESTful API for creating auctions and placing bids
- Support for different auction types
- Persistence using JSON files
- JWT-based authentication
- Domain-driven design with clean separation of concerns

## Getting Started

### Prerequisites

- Go 1.18 or newer

### Installation

1. Clone the repository
```bash
git clone https://github.com/wallymathieu/auction-site-go.git
cd auction-site-go
```

2. Install dependencies
```bash
go mod download
```

3. Build the application
```bash
go build -o auction-site ./cmd/server
```

### Running the Server

```bash
./auction-site
```

The server will start on port 8080.

## API Endpoints

### Authentication

All write operations require authentication via the `x-jwt-payload` header. Note that the `x-jwt-payload` header is a decoded JWT and not an actual JWT, since this app is supposed to be deployed behind a front-proxy.

Example JWT payload format for a buyer/seller:
```json
{
  "sub": "a1",
  "name": "Test User",
  "u_typ": "0"
}
```

Example JWT payload format for support:
```json
{
  "sub": "s1",
  "u_typ": "1"
}
```

The JWT payload should be Base64 encoded when sent in the header.

### Endpoints

- `GET /auctions` - List all auctions
- `GET /auctions/:id` - Get auction details, including bids and winner information if available
- `POST /auctions` - Create a new auction
- `POST /auctions/:id/bids` - Place a bid on an auction

### Example Requests

#### Create an auction

```bash
curl -X POST http://localhost:8080/auctions \
  -H "Content-Type: application/json" \
  -H "x-jwt-payload: eyJzdWIiOiJhMSIsICJuYW1lIjoiVGVzdCIsICJ1X3R5cCI6IjAifQo=" \
  -d '{
    "id": 1,
    "startsAt": "2023-01-01T10:00:00.000Z",
    "endsAt": "2023-12-31T10:00:00.000Z",
    "title": "Test Auction",
    "currency": "VAC"
  }'
```

#### Place a bid

```bash
curl -X POST http://localhost:8080/auctions/1/bids \
  -H "Content-Type: application/json" \
  -H "x-jwt-payload: eyJzdWIiOiJhMiIsICJuYW1lIjoiQnV5ZXIiLCAidV90eXAiOiIwIn0K=" \
  -d '{
    "amount": 100
  }'
```

## Domain Model

### Core Types

- `Auction` - Represents an auction with ID, title, start/end times, seller, type, and currency
- `Bid` - Represents a bid on an auction with auction ID, bidder, time, and amount
- `State` - Interface for different auction state implementations
- `Command` - Interface for commands that can be executed against the system
- `Event` - Interface for events generated as a result of commands

### State Machine

Each auction type implements a state machine:

#### Timed Ascending (English)
- `AwaitingStartState` - Auction hasn't started yet
- `OngoingState` - Auction is active and accepting bids
- `EndedState` - Auction has ended

#### Single Sealed Bid (Blind/Vickrey)
- `SealedBidState` - Accepts bids until the expiry time
- After expiry, bids are disclosed and the winner is determined

## Testing

Run the tests with:

```bash
go test ./...
```

## Development

The codebase follows a clean architecture with the following layers:

- **Domain** - Core business logic and types
- **Persistence** - Data storage
- **Web** - HTTP API and request handling

### Project Structure

```
auction-site-go/
├── cmd/
│   └── server/         # Entry point for the application
├── internal/
│   ├── domain/         # Domain models and business logic
│   ├── persistence/    # Data storage
│   └── web/            # HTTP API
└── tests/              # Integration tests
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
