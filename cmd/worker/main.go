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

	const consumeQueuedJobQuery = `
		update worker_job wj set
			status = $1
		from
			wallet w
		where
			wj.status = $2 and
			wj.type = 0 and
			w.id = (wj.data->>'walletId')::integer and
			w.delete_scheduled = false
		returning
			wj.id, 
			wj.user_account_id, 
			(wj.data->>'fresh')::boolean, 
			(wj.data->>'walletId')::integer, 
			(wj.data->>'walletAddress')::varchar, 
			w.data,
			w.network
	`
	row := pool.QueryRow(
		context.Background(), consumeQueuedJobQuery,
		db.WorkerJobInProgress, db.WorkerJobQueued,
	)

	var (
		jobId                uuid.UUID
		userAccountId        int32
		fresh                bool
		walletId             int32
		walletAddress        string
		walletDataSerialized json.RawMessage
		network              db.Network
	)

	if err := row.Scan(
		&jobId, &userAccountId,
		&fresh, &walletId, &walletAddress,
		&walletDataSerialized, &network,
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

			const selectJobScheduledForCancelQuery = `
				select 1 from worker_job where
					id = $1 and status = $2
			`
			tag, err := pool.Exec(
				context.Background(), selectJobScheduledForCancelQuery,
				jobId, db.WorkerJobCancelScheduled,
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

	err := errgroup.Wait()

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
