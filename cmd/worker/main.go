package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"taxee/cmd/fetcher"
	"taxee/cmd/fetcher/evm"
	"taxee/cmd/fetcher/solana"
	"taxee/cmd/parser"
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

// insertWorkerResultQuery
//
//	insert into worker_result (
//		type, canceled, error_message, started_at, finished_at, data
//	) values (
//		$1, $2, $3, $4, now(), $5
//	)
const insertWorkerResultQuery string = `
	insert into worker_result (
		type, canceled, error_message, started_at, finished_at, data
	) values (
		$1, $2, $3, $4, now(), $5
	)
`

type workerState struct {
	runningFetchWallets      atomic.Uint32
	runningFetchTransactions atomic.Uint32
}

func consumeQueuedWallets(
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
				"fetch_wallet", canceled, errorMessage, startedAt, workerDataSerialized,
			)
			if err != nil {
				logger.Error("unable to insert worker data: %s", err.Error())
				return err
			}

			return nil
		},
	)
}

func deleteWallets(pool *pgxpool.Pool) {
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
}

func consumeQueuedWorkerJob(
	ctx context.Context,
	pool *pgxpool.Pool,
	evmClient *evm.Client,
	state *workerState,
) {
	state.runningFetchTransactions.Add(1)
	defer state.runningFetchTransactions.Store(state.runningFetchTransactions.Load() - 1)

	const consumeQueuedJobQuery = `
		update worker_job set
			status = 'in_progress'
		where
			id = (
				select id from worker_job where
					status = 'queued'
				order by
					queued_at asc
				limit
					1
			)
		returning
			id, type, user_account_id
	`
	row := pool.QueryRow(context.Background(), consumeQueuedJobQuery)
	var jobId, userAccountId int32
	var jobType string
	if err := row.Scan(&jobId, &jobType, &userAccountId); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return
		}
		logger.Error("unable to consume queued job: %s", err.Error())
		return
	}

	logger.Info("consumed %s job %d", jobType, jobId)
	startedAt := time.Now()
	groupCtx, groupCtxCancel := context.WithCancel(ctx)

	var eg errgroup.Group

	eg.Go(func() error {
		defer groupCtxCancel()
		switch jobType {
		case "parse_transactions":
			return parser.ParseTransactions(groupCtx, pool, userAccountId, evmClient)
		default:
			return fmt.Errorf("invalid type: %s", jobType)
		}
	})

	eg.Go(func() error {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			const selectJobScheduledForCancelQuery = `
				select 1 from worker_job where
					id = $1 and status = 'cancel_scheduled'
			`
			tag, err := pool.Exec(
				groupCtx, selectJobScheduledForCancelQuery,
				jobId,
			)
			if err != nil {
				// job is done
				if errors.Is(err, context.Canceled) {
					return nil
				}

				// NOTE: should this return an error and cancel the process ?
				logger.Error("unable to query cancel message: %s", err)
				continue
			}

			if tag.RowsAffected() > 0 {
				logger.Info("canceled job: %d", jobId)
				groupCtxCancel()
				return groupCtx.Err()
			}
		}
	})

	err := eg.Wait()
	logger.Info("job %d done", jobId)

	var status db.Status
	var canceled bool
	var errorMessage pgtype.Text

	switch {
	case err == nil:
		status = db.StatusSuccess
	case errors.Is(err, context.Canceled):
		status = db.StatusCanceled
		canceled = true
	default:
		logger.Info("job error: %s", err.Error())
		status = db.StatusError
		errorMessage = pgtype.Text{
			Valid:  true,
			String: err.Error(),
		}
	}

	const updateJobQuery = `
		update worker_job set
			status = $1,
			finished_at = now()
		where
			id = $2
	`

	err = db.ExecuteTx(ctx, pool, func(ctx context.Context, tx pgx.Tx) error {
		batch := pgx.Batch{}

		batch.Queue(updateJobQuery, status, jobId)

		type workerData struct {
			UserAccountId int32 `json:"userAccountId"`
		}
		workerDataSerialized, err := json.Marshal(workerData{
			UserAccountId: userAccountId,
		})
		if err != nil {
			return fmt.Errorf("unable to serialize worker data: %w", err)
		}

		batch.Queue(
			insertWorkerResultQuery,
			"parse_transactions", canceled, errorMessage, startedAt,
			workerDataSerialized,
		)

		return tx.SendBatch(ctx, &batch).Close()
	})

	if err != nil {
		logger.Error("unable to update job: %s", err)
	}

	logger.Info("updated db")
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
			go consumeQueuedWallets(pool, solanaRpc, evmClient, &state)
		}

		if state.runningFetchTransactions.Load() == 0 {
			go consumeQueuedWorkerJob(context.TODO(), pool, evmClient, &state)
		}

		go deleteWallets(pool)
	}
}
