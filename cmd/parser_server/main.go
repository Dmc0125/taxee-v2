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

// getSyncRequestQuery
//
//	select user_account_id, type from
//		sync_request
//	where
//		status = 0 and (type != $1)
//	order by
//		timestamp asc
//	limit
//		1
const getSyncRequestQuery string = `
	select id, user_account_id, type from 
		sync_request
	where
		status = 0
	order by
		timestamp asc
	limit
		1
`

// updateSyncRequestStatusQuery
//
//	update sync_request set
//		status = $2
//	where
//		id = $1
const updateSyncRequestStatusQuery string = `
	update sync_request set
		status = $2
	where
		id = $1
`

// deleteSyncRequestQuery
//
//	delete from sync_request where id = $1
const deleteSyncRequestQuery string = `
	delete from sync_request where id = $1
`

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
	_ = ctxCancel

	ticker := time.NewTicker(1 * time.Second)
	for {
		<-ticker.C

		var (
			requestId   int32
			requestType uint8
		)

		switch {
		// idle state
		case userAccountId < 0:
			row := pool.QueryRow(context.Background(), getSyncRequestQuery)
			err := row.Scan(&requestId, &userAccountId, &requestType)
			if errors.Is(err, pgx.ErrNoRows) {
				logger.Info("queue empty")
				continue
			}
			assert.NoErr(err, "unable to query sync request")

			// TODO: Set based on operation
			switch requestType {
			case 0:
				// fetch + parse
			case 1:
				_, err = pool.Exec(
					context.Background(),
					updateSyncRequestStatusQuery,
					requestId, db.SyncRequestProcessing,
				)
				assert.NoErr(err, "unable to update sync request")

				// parse
				var ctx context.Context
				ctx, ctxCancel = context.WithCancel(context.Background())

				logger.Info("received parse request for %d", userAccountId)
				parser.Parse(ctx, pool, evmClient, userAccountId, true)

				_, err = pool.Exec(context.Background(), deleteSyncRequestQuery, requestId)
				assert.NoErr(err, "unable to delete sync request")

				userAccountId = -1
			}
		// processing
		default:
			// const getSyncRequestQuery = `
			// 	delete from sync_request where id = (
			// 		select id from sync_request where
			// 			status = 0 and
			// 			type = 2 and
			// 			user_account_id = $1
			// 		limit
			// 			1
			// 	) returning id
			// `
			// row := pool.QueryRow(
			// 	context.Background(),
			// 	getSyncRequestQuery,
			// 	userAccountId,
			// )
			// var _x int
			// err := row.Scan(&_x)
			// if errors.Is(err, pgx.ErrNoRows) {
			// 	continue
			// }
			//
			// ctxCancel()
			// userAccountId = -1
		}
	}
}
