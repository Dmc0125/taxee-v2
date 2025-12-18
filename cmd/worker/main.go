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
		type, status, error_message, started_at, finished_at, data
	) values (
		$1, $2, $3, $4, now(), $5
	)
`

// selectJobScheduledForCancelQuery
//
//	select 1 from worker_job where
//		id = $1 and status = 'cancel_scheduled'
const selectJobScheduledForCancelQuery string = `
	select status from worker_job where
		id = $1
`

var workerErrResetScheduled = errors.New("reset scheduled")

func cancelResourceJob(
	appCtx context.Context,
	jobCancel context.CancelFunc,
	pool *pgxpool.Pool,
	query string,
	resourceId int32,
) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		var status db.Status
		err := pool.QueryRow(appCtx, query, resourceId).Scan(&status)
		switch {
		case err == nil:
			switch status {
			case db.StatusSuccess, db.StatusError:
				return nil
			case db.StatusCancelScheduled:
				logger.Info("canceled job: %d", resourceId)
				jobCancel()
				return context.Canceled
			case db.StatusResetScheduled:
				logger.Info("reset job: %d", resourceId)
				jobCancel()
				return workerErrResetScheduled
			}
		case errors.Is(err, context.Canceled):
			return nil
		case !errors.Is(err, pgx.ErrNoRows):
			logger.Error("unable to query resource scheduled for cancel: %s", err)
		}
	}
}

func consumeQueuedWallets(
	jobCounter *atomic.Uint32,
	ctx context.Context,
	pool *pgxpool.Pool,
	solanaRpc *solana.Rpc,
	evmClient *evm.Client,
) {
	jobCounter.Add(1)
	defer jobCounter.Store(jobCounter.Load() - 1)

	// only consume wallet for which no other jobs are running
	const consumeQueuedWalletQuery = `
		update wallet set
			status = 'in_progress'
		where
			id = (
				select id from wallet w where
					status = 'queued' and
					delete_scheduled = false and
					not exists (
						select 1 from worker_job wj where
							wj.user_account_id = w.id and
							wj.status in ('in_progress', 'cancel_scheduled', 'reset_scheduled')
					)
				order by
					queued_at asc
				limit
					1
			)
		returning
			id, user_account_id, address, network, fresh, data
	`
	row := pool.QueryRow(ctx, consumeQueuedWalletQuery)

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

	var eg errgroup.Group
	groupCtx, groupCtxCancel := context.WithCancel(ctx)

	startedAt := time.Now()

	eg.Go(func() error {
		defer groupCtxCancel()
		return fetcher.Fetch(
			groupCtx, pool, solanaRpc, evmClient,
			userAccountId, network, walletAddress, walletId,
			walletDataSerialized, fresh,
		)
	})

	eg.Go(func() error {
		const selectWalletScheduledForCancelQuery = `
			select status  from wallet where
				id = $1
		`
		return cancelResourceJob(
			groupCtx, groupCtxCancel, pool,
			selectWalletScheduledForCancelQuery, walletId,
		)
	})

	workerError := eg.Wait()

	db.ExecuteTx(
		ctx, pool,
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
			var errorMessage pgtype.Text

			switch {
			case workerError == nil:
				status = db.StatusSuccess
			case errors.Is(workerError, context.Canceled):
				status = db.StatusCanceled
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
				"fetch_wallet", status, errorMessage, startedAt, workerDataSerialized,
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
	jobCounter *atomic.Uint32,
	ctx context.Context,
	pool *pgxpool.Pool,
	evmClient *evm.Client,
	jobType db.JobType,
) {
	jobCounter.Add(1)
	defer jobCounter.Store(jobCounter.Load() - 1)

	var query string
	switch jobType {
	case db.JobParseTransactions:
		// only consume parse_transactions job when every wallet is in terminal state
		query = `
			update worker_job set
				status = 'in_progress'
			where
				id = (
					select wj.id from worker_job wj where
						wj.status = 'queued' and
						wj.type = 'parse_transactions' and
						not exists (
							select 1 from wallet w where
								w.user_account_id = wj.user_account_id and
								w.status in ('queued', 'in_progress', 'cancel_scheduled')
						)
					order by
						queued_at asc
					limit
						1
				)
			returning
				id, user_account_id
		`
	case db.JobParseEvents:
		// only consume parse_events job when parse_transactions was successful
		query = `
			update worker_job set
				status = 'in_progress'
			where
				id = (
					select wj.id from worker_job wj where
						status = 'queued' and
						wj.type = 'parse_events' and
						(
							select wj2.status from worker_job wj2 where
								wj2.user_account_id = wj.user_account_id and
								wj2.type = 'parse_transactions'
						) = 'success'
					order by
						queued_at asc
					limit
						1
				)
			returning
				id, user_account_id
		`
	default:
		assert.True(false, "invalid job type: %s", jobType)
	}

	row := pool.QueryRow(context.Background(), query)
	var jobId, userAccountId int32
	if err := row.Scan(&jobId, &userAccountId); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return
		}
		logger.Error("unable to consume queued %s job: %s", jobType, err.Error())
		return
	}

	logger.Info("consumed %s job %d", jobType, jobId)
	startedAt := time.Now()
	groupCtx, groupCtxCancel := context.WithCancel(ctx)

	var eg errgroup.Group

	eg.Go(func() error {
		defer groupCtxCancel()
		switch jobType {
		case db.JobParseTransactions:
			return parser.ParseTransactions(groupCtx, pool, userAccountId, evmClient)
		case db.JobParseEvents:
			return parser.ParseEvents(groupCtx, pool, userAccountId)
		default:
			assert.True(false, "unreachable")
			return nil
		}
	})

	eg.Go(func() error {
		return cancelResourceJob(
			groupCtx, groupCtxCancel, pool,
			selectJobScheduledForCancelQuery, jobId,
		)
	})

	err := eg.Wait()
	logger.Info("%s job %d done", jobType, jobId)

	var status db.Status
	var errorMessage pgtype.Text

	switch {
	case err == nil:
		status = db.StatusSuccess
	case errors.Is(err, workerErrResetScheduled):
		status = db.StatusQueued
	case errors.Is(err, context.Canceled):
		status = db.StatusCanceled
	default:
		logger.Info("%s job error: %s", jobType, err.Error())
		status = db.StatusError
		errorMessage = pgtype.Text{
			Valid:  true,
			String: err.Error(),
		}
	}

	// NOTE:
	// status has to be updated conditionally because - if job finishes successfuly
	// and cancels the goroutine that pulls the status and in between the time
	// when the goroutine is not pulling anymore and this update happens, wallet
	// is added/refetched/canceled, ... whatever, job would not reset, since that
	// state would not be known
	const updateJobQuery = `
		update worker_job set
			status = (
				case
					when status = 'reset_scheduled' then 'queued'
					else $1
				end
			)::status,
			finished_at = now()
		where
			id = $2
	`

	err = db.ExecuteTx(ctx, pool, func(ctx context.Context, tx pgx.Tx) error {
		batch := pgx.Batch{}

		// TODO: this needs to check if job is not scheduled for reset
		batch.Queue(updateJobQuery, status, jobId)

		type workerData struct {
			UserAccountId int32 `json:"userAccountId"`
		}
		workerDataSerialized, err := json.Marshal(workerData{
			UserAccountId: userAccountId,
		})
		if err != nil {
			return fmt.Errorf("unable to serialize %s worker data: %w", jobType, err)
		}

		batch.Queue(
			insertWorkerResultQuery,
			jobType, status, errorMessage, startedAt,
			workerDataSerialized,
		)

		return tx.SendBatch(ctx, &batch).Close()
	})

	if err != nil {
		logger.Error("unable to update %s job: %s", jobType, err)
	}

	logger.Info("updated %s", jobType)
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

	var (
		runningFetchWallets      atomic.Uint32
		runningParseTransactions atomic.Uint32
		runningParseEvents       atomic.Uint32
	)

	for {
		<-ticker.C

		if runningFetchWallets.Load() == 0 {
			go consumeQueuedWallets(
				&runningFetchWallets,
				context.TODO(),
				pool, solanaRpc, evmClient,
			)
		}

		if runningParseTransactions.Load() == 0 {
			go consumeQueuedWorkerJob(
				&runningParseTransactions,
				context.TODO(),
				pool, evmClient, db.JobParseTransactions,
			)
		}

		if runningParseEvents.Load() == 0 {
			go consumeQueuedWorkerJob(
				&runningParseEvents,
				context.TODO(),
				pool, evmClient, db.JobParseEvents,
			)
		}

		go deleteWallets(pool)
	}
}
