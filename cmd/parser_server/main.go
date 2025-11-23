package main

import (
	"context"
	"errors"
	"os"
	"taxee/cmd/fetcher/evm"
	"taxee/cmd/parser"
	"taxee/pkg/assert"
	"taxee/pkg/coingecko"
	"taxee/pkg/db"
	"taxee/pkg/dotenv"
	"taxee/pkg/logger"
	requesttimer "taxee/pkg/request_timer"
	"time"

	"github.com/jackc/pgx/v5"
)

// this server handles 2 things
// 	1. fetching transactions for wallets
// 	2. parsing the already fetched transactions
//
// request flow:
// 	1. user clicks on some button which posts to /sync-request
// 	2. /sync-request insert the request to the db
// 	3. this server pulls the request and processes it
//
// if user creates any subsequent requests, the request type will be updated to
// match the latest one (timestamp update ??)
//
// if user creates cancel request, all requests belonging to user will be deleted
// and cancel request will be created, the current request process will be stopped
//
// at one point there can only be **one** request from a single user

func main() {
	appEnv := os.Getenv("APP_ENV")
	if appEnv != "prod" {
		assert.NoErr(dotenv.ReadEnv(), "")
	}

	logger.Info("Initializing db pool")
	pool, err := db.InitPool(context.Background(), appEnv)
	assert.NoErr(err, "")

	logger.Info("Initializing evm client")
	alchemyReqTimer := requesttimer.NewDefault(100)
	evmClient := evm.NewClient(alchemyReqTimer)

	logger.Info("Initializing coingecko")
	coingecko.Init()

	// if the server is in idle state -> query the sync request
	// if the server is in processing state -> query the cancel request for
	//	current user

	userAccountId := int32(-1)
	var ctxCancel context.CancelFunc

	ticker := time.NewTicker(1 * time.Second)
	for {
		<-ticker.C

		var (
			timestamp   time.Time
			requestType uint8
		)

		switch {
		// idle state
		case userAccountId < -1:
			const getSyncRequestQuery = `
				select 
					timestamp,
					user_account_id, 
					type
				from
					sync_request
				where
					status = 0 and (type = 0 or type = 1)
				order by
					timestamp asc
				limit
					1
			`
			row := pool.QueryRow(context.Background(), getSyncRequestQuery)
			err := row.Scan(&timestamp, &userAccountId, &requestType)
			if errors.Is(err, pgx.ErrNoRows) {
				logger.Info("queue empty")
				userAccountId = -1
				continue
			}
			assert.NoErr(err, "unable to query sync request")

			switch requestType {
			case 0:
				// fetch + parse
			case 1:
				// parse
				var ctx context.Context
				ctx, ctxCancel = context.WithCancel(context.Background())

				logger.Info("received parse request for %d", userAccountId)
				parser.Parse(ctx, pool, evmClient, userAccountId, true)
			}
		// processing
		default:
			const getSyncRequestQuery = `
				select 1 from sync_request where
					status = 0 and 
					type = 2 and
					user_account_id = $1
				limit 
					1
			`
			row := pool.QueryRow(
				context.Background(),
				getSyncRequestQuery,
				userAccountId,
			)
			var _x int
			err := row.Scan(&_x)
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}

			ctxCancel()
			userAccountId = -1
		}
	}
}
