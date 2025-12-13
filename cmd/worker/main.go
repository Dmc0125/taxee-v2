package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync/atomic"
	"taxee/cmd/fetcher"
	"taxee/cmd/fetcher/evm"
	"taxee/cmd/fetcher/solana"
	"taxee/pkg/assert"
	"taxee/pkg/coingecko"
	"taxee/pkg/db"
	"taxee/pkg/dotenv"
	"taxee/pkg/logger"
	requesttimer "taxee/pkg/request_timer"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

type workerState struct {
	runningFetchWallets atomic.Uint32
}

func tryWalletFetch(
	pool *pgxpool.Pool,
	solanaRpc *solana.Rpc,
	evmClient *evm.Client,
	state *workerState,
) {
	state.runningFetchWallets.Add(1)
	defer state.runningFetchWallets.Store(state.runningFetchWallets.Load() - 1)

	const consumeQueuedWalletQuery = `
		update wallet set
			status = 'in_progress'
		where
			id = (
				select id from wallet w where
					status = 'queued' and
					delete_scheduled = false
				order by
					queued_at asc
				limit
					1
			)
		returning
			id, user_account_id, address, network, fresh, data
	`
	row := pool.QueryRow(context.Background(), consumeQueuedWalletQuery)

	var (
		walletId, userAccountId int32
		walletAddress           string
		network                 db.Network
		fresh                   bool
		walletDataSerialized    json.RawMessage
	)

	if err := row.Scan(
		&walletId, &userAccountId, &walletAddress,
		&network, &fresh, &walletDataSerialized,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return
		}
		logger.Error("unable to consume queued job: %s", err.Error())
		return
	}

	fetchCtx, fetchCtxCancel := context.WithCancel(context.Background())

	var errgroup errgroup.Group
	groupCtx, groupCtxCancel := context.WithCancel(context.Background())

	startedAt := time.Now()

	errgroup.Go(func() error {
		err := fetcher.Fetch(
			fetchCtx, pool, solanaRpc, evmClient,
			userAccountId, network, walletAddress, walletId,
			walletDataSerialized, fresh,
		)
		groupCtxCancel()
		return err
	})

	errgroup.Go(func() error {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-groupCtx.Done():
				return nil
			case <-ticker.C:
			}

			const selectWalletScheduledForCancelQuery = `
				select 1 from wallet where
					id = $1 and status = 'cancel_scheduled'
			`
			tag, err := pool.Exec(
				context.Background(), selectWalletScheduledForCancelQuery,
				walletId,
			)
			if err != nil {
				// NOTE: should this return an error and cancel the process ?
				logger.Error("unable to query cancel message: %s", err)
				continue
			}

			if tag.RowsAffected() > 0 {
				logger.Info("canceled job for wallet: %d", walletId)
				fetchCtxCancel()
				return fetchCtx.Err()
			}
		}
	})

	workerError := errgroup.Wait()

	db.ExecuteTx(
		context.Background(), pool,
		func(ctx context.Context, tx pgx.Tx) error {
			const updateWalletQuery = `
				update wallet set
					status = $1, finished_at = now()
				where
					id = $2
				returning
					tx_count
			`
			const insertWorkerResultQuery = `
				insert into worker_result (
					type, canceled, error_message, started_at, finished_at, data
				) values (
					'fetch_wallet', $1, $2, $3, now(), $4
				)
			`
			var status db.Status
			var canceled bool
			var errorMessage pgtype.Text

			switch {
			case workerError == nil:
				status = db.StatusSuccess
			case errors.Is(workerError, context.Canceled):
				status = db.StatusCanceled
				canceled = true
			default:
				status = db.StatusError
				errorMessage = pgtype.Text{
					Valid:  true,
					String: workerError.Error(),
				}
			}

			row := tx.QueryRow(ctx, updateWalletQuery, status, walletId)
			var txCount int32
			if err := row.Scan(&txCount); err != nil {
				logger.Error("unable to update wallet: %s", err.Error())
				return err
			}

			type workerData struct {
				WalletId      int32      `json:"walletId"`
				WalletAddress string     `json:"walletAddress"`
				Network       db.Network `json:"network"`
				TxCount       int32      `json:"txCount"`
			}
			workerDataSerialized, err := json.Marshal(workerData{
				WalletId:      walletId,
				WalletAddress: walletAddress,
				Network:       network,
				TxCount:       txCount,
			})
			assert.NoErr(err, "unable to marshal worker data")

			_, err = tx.Exec(
				ctx, insertWorkerResultQuery,
				canceled, errorMessage, startedAt, workerDataSerialized,
			)
			if err != nil {
				logger.Error("unable to insert worker data: %s", err.Error())
				return err
			}

			return nil
		},
	)
}

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
	solanaRpc := solana.NewRpc(alchemyReqTimer)

	logger.Info("Initializing coingecko")
	coingecko.Init()

	ticker := time.NewTicker(time.Second)
	state := workerState{}

	for {
		<-ticker.C

		if state.runningFetchWallets.Load() == 0 {
			go tryWalletFetch(pool, solanaRpc, evmClient, &state)
		}

		///////////////////
		// try delete wallet
		func() {
			const selectWalletsScheduledForDelete = `
				delete from wallet where
					status != 'cancel_scheduled' and
					delete_scheduled = true
				returning
					user_account_id, id
			`
			rows, err := pool.Query(context.Background(), selectWalletsScheduledForDelete)
			if err != nil {
				logger.Error("unable to delete wallets: %s", err)
				return
			}

			for rows.Next() {
				var userAccountId, walletId int32
				if err := rows.Scan(&userAccountId, &walletId); err != nil {
					logger.Error("unable to scan wallet id: %s", err)
					return
				}
				logger.Info("deleted wallet %d for user %d", walletId, userAccountId)
			}
		}()

	}
}
