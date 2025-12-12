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

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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

	tx, err := pool.Begin(context.Background())
	if err != nil {
		logger.Error("unable to begin tx: %s", err.Error())
		return
	}
	defer tx.Rollback(context.Background())

	// TODO: there is a bug where this starts fethching when wallet is scheduled for delete
	const consumeQueuedJobQuery = `
		update worker_job set
			status = 1
		where
			status = 0 and
			type = 0
		returning
			id, data, user_account_id
	`
	row := tx.QueryRow(context.Background(), consumeQueuedJobQuery)

	var jobId uuid.UUID
	var userAccountId int32
	var dataSerialized json.RawMessage
	if err := row.Scan(&jobId, &dataSerialized, &userAccountId); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return
		}
		logger.Error("unable to consume queued job: %s", err.Error())
		return
	}

	// TODO: validate data
	var data db.WorkerJobFetchWalletData
	if err := json.Unmarshal(dataSerialized, &data); err != nil {
		logger.Error("unable to unmarshal job data: %s", err.Error())
		return
	}

	const selectWalletDataQuery = "select data from wallet where id = $1"
	var walletDataSerialized json.RawMessage
	row = tx.QueryRow(context.Background(), selectWalletDataQuery, data.WalletId)
	if err := row.Scan(&walletDataSerialized); err != nil {
		logger.Error("unable to select wallet: %s", err.Error())
		return
	}

	if err = tx.Commit(context.Background()); err != nil {
		logger.Error("unable to commit: %s", err.Error())
		return
	}

	fetchCtx, fetchCtxCancel := context.WithCancel(context.Background())

	var errgroup errgroup.Group
	groupCtx, groupCtxCancel := context.WithCancel(context.Background())

	errgroup.Go(func() error {
		err := fetcher.Fetch(
			fetchCtx, pool, solanaRpc, evmClient,
			userAccountId, data.Network, data.WalletAddress, data.WalletId,
			walletDataSerialized, data.Fresh,
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

			const selectJobScheduledForCancelQuery = `
				select 1 from worker_job where
					id = $1 and status = 4 
			`
			tag, err := pool.Exec(
				context.Background(), selectJobScheduledForCancelQuery,
				jobId,
			)
			if err != nil {
				// NOTE: should this return an error and cancel the process ?
				logger.Error("unable to query cancel message: %s", err)
				continue
			}

			if tag.RowsAffected() > 0 {
				logger.Info("canceled job: %s", jobId)
				fetchCtxCancel()
				return fetchCtx.Err()
			}
		}
	})

	err = errgroup.Wait()

	const updateWorkerJobQuery = `
		update worker_job set
			status = $1
		where
			id = $2
	`
	const insertWorkerErrorQuery = `
		insert into worker_error (
			job_id, error_message
		) values (
			$1, $2
		)
	`
	batch := pgx.Batch{}

	switch {
	case err == nil:
		batch.Queue(updateWorkerJobQuery, db.WorkerJobSuccess, jobId)
	case errors.Is(err, context.Canceled):
		batch.Queue(updateWorkerJobQuery, db.WorkerJobCanceled, jobId)
	default:
		batch.Queue(updateWorkerJobQuery, db.WorkerJobError, jobId)
		batch.Queue(insertWorkerErrorQuery, jobId, err.Error())
	}

	err = db.ExecuteTx(
		context.Background(), pool,
		func(ctx context.Context, tx pgx.Tx) error {
			br := tx.SendBatch(ctx, &batch)
			return br.Close()
		},
	)
	if err != nil {
		logger.Error("unable set worker result: %s", err)
	}
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
			tx, err := pool.Begin(context.Background())
			if err != nil {
				logger.Error("unable to begin tx: %s", err)
				return
			}
			defer tx.Rollback(context.Background())

			// select wallets which are scheduled for delete and do not have
			// a job thats is currently running or is scheduled for cancel
			const selectWalletsScheduledForDelete = `
				select user_account_id, id from wallet w where
					w.delete_scheduled = true and not exists (
						select 1 from worker_job wj where
							(wj.status = 1 or wj.status = 4) and
							(wj.data->>'walletId')::integer = w.id 
					)
			`
			rows, err := tx.Query(context.Background(), selectWalletsScheduledForDelete)
			if err != nil {
				logger.Error("unable to delete wallets: %s", err)
				return
			}

			batch := pgx.Batch{}

			for rows.Next() {
				var userAccountId, walletId int32
				if err := rows.Scan(&userAccountId, &walletId); err != nil {
					logger.Error("unable to scan wallet id: %s", err)
					return
				}

				logger.Info("deleting wallet %d for user %d", walletId, userAccountId)

				const deleteWalletQuery = "call delete_wallet($1, $2)"
				batch.Queue(deleteWalletQuery, userAccountId, walletId)

				const deleteJobQuery = `
					delete from worker_job wj where 
						(wj.data->>'walletId')::integer = $1 and
						wj.user_account_id = $2
				`
				batch.Queue(deleteJobQuery, walletId, userAccountId)
			}

			if err := rows.Err(); err != nil {
				logger.Error("unable to read rows: %s", err)
				return
			}

			if batch.Len() > 0 {
				br := tx.SendBatch(context.Background(), &batch)
				if err := br.Close(); err != nil {
					logger.Error("unable to delete wallets: %s", err)
					return
				}
			}

			if err := tx.Commit(context.Background()); err != nil {
				logger.Error("unable to commit: %s", err)
			}

			if batch.Len() > 0 {
				logger.Info("wallets deleted")
			}
		}()

	}
}
