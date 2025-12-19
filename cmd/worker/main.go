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
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

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
			status = 'in_progress',
			started_at = now()
		where
			id = (
				select id from wallet w where
					status = 'queued'
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

	eg.Go(func() error {
		defer groupCtxCancel()
		return fetcher.Fetch(
			groupCtx, pool, solanaRpc, evmClient,
			userAccountId, network, walletAddress, walletId,
			walletDataSerialized, fresh,
		)
	})

	eg.Go(func() error {
		const selectWalletStatus = `
			select status from wallet where
				id = $1
		`
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			<-ticker.C

			var status db.Status
			err := pool.QueryRow(groupCtx, selectWalletStatus, walletId).Scan(&status)
			switch {
			case errors.Is(err, context.Canceled):
				return nil
			case errors.Is(err, pgx.ErrNoRows):
				continue
			case err != nil:
				logger.Error("unable to query resource scheduled for cancel: %s", err)
				continue
			}

			if status == db.StatusDelete {
				groupCtxCancel()
				return context.Canceled
			}
		}
	})

	workerError := eg.Wait()

	db.ExecuteTx(
		ctx, pool,
		func(ctx context.Context, tx pgx.Tx) error {
			const deleteWallet = `
				delete from wallet where id = $1 and status = 'delete'
			`
			const updateWallet = `
				update wallet set
					status = $1, finished_at = now()
				where
					id = $2
				returning
					tx_count
			`

			tag, err := tx.Exec(ctx, deleteWallet, walletId)
			if err != nil {
				logger.Error("unable to finish fetch wallet: %s", err)
				return err
			}
			if tag.RowsAffected() == 1 {
				logger.Error("wallet deleted: %d", walletId)
				return nil
			}

			var status db.Status

			switch workerError {
			case nil:
				status = db.StatusSuccess
			default:
				logger.Error("unable to fetch wallet: %s", workerError.Error())
				status = db.StatusError
			}

			row := tx.QueryRow(ctx, updateWallet, status, walletId)
			var txCount int32
			if err := row.Scan(&txCount); err != nil {
				logger.Error("unable to update wallet: %s", err.Error())
				return err
			}

			return nil
		},
	)
}

func consumeQueuedParseTransactions(
	jobCounterPt *atomic.Uint32,
	jobCounterPe *atomic.Uint32,
	ctx context.Context,
	pool *pgxpool.Pool,
	evmClient *evm.Client,
) {
	jobCounterPt.Add(1)
	defer jobCounterPt.Store(jobCounterPt.Load() - 1)

	jobCounterPe.Add(1)
	defer jobCounterPe.Store(jobCounterPe.Load() - 1)

	const consume = `
		update parser set
			status = 'pt_in_progress',
			started_at = now()
		where
			id = (
				select p.id from parser p where
					p.status = 'pt_queued' and
					not exists (
						select 1 from wallet w where
							w.user_account_id = p.user_account_id and
							w.status in ('queued', 'in_progress', 'delete')
					)
				order by
					p.queued_at asc
				limit
					1
			)
		returning
			id, user_account_id
	`
	var parserId, userAccountId int32
	err := pool.QueryRow(ctx, consume).Scan(&parserId, &userAccountId)
	if errors.Is(err, pgx.ErrNoRows) {
		return
	}
	if err != nil {
		logger.Error("unable to query: %s", err)
		return
	}

	var eg errgroup.Group
	groupCtx, groupCtxCancel := context.WithCancel(ctx)

	var errPt = errors.New("parse_transactions")
	var errPe = errors.New("parse_events")

	// parse
	eg.Go(func() error {
		defer groupCtxCancel()
		if err := parser.ParseTransactions(groupCtx, pool, userAccountId, evmClient); err != nil {
			logger.Error("unable to parse transactios: %s", err)
			return fmt.Errorf("%w: %w", errPt, err)
		}

		const updateParser = `
			update parser set
				status = 'pe_in_progress'
			where
				id = $1 and
				status = 'pt_in_progress'
		`
		if _, err := pool.Exec(groupCtx, updateParser, parserId); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			logger.Error("unable to update parser: %s", err)
			return err
		}

		if err := parser.ParseEvents(groupCtx, pool, userAccountId); err != nil {
			logger.Error("unable to parse events: %s", err)
			return fmt.Errorf("%w: %w", errPe, err)
		}

		return nil
	})

	// TODO:
	// var errPTReset = errors.New("pt_reset")
	// var errPEReset = errors.New("pe_reset")
	//
	// listen for reset
	eg.Go(func() error {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			<-ticker.C

			const selectReset = `
				select status from parser where
					id = $1 and
					status = 'reset'
			`
			var status db.ParserStatus
			err := pool.QueryRow(groupCtx, selectReset, parserId).Scan(&status)
			if errors.Is(err, context.Canceled) {
				return nil
			}
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			if err != nil {
				logger.Error("unable to select reset parser: %s", err)
				continue
			}

			groupCtxCancel()
			logger.Info("parser reset")

			// switch status {
			// }

			return groupCtx.Err()
		}
	})

	parserErr := eg.Wait()

	var status db.ParserStatus

	switch {
	case parserErr == nil:
		status = db.ParserStatusSuccess
	case errors.Is(parserErr, context.Canceled):
		status = db.ParserStatusPTQueued
	case errors.Is(parserErr, errPt):
		status = db.ParserStatusPTError
	case errors.Is(parserErr, errPe):
		status = db.ParserStatusPEError
	default:
		assert.True(false, "unreachable")
	}

	const finishParser = `
		update parser set
			finished_at = now(),
			status = $1
		where
			id = $2
	`
	if _, err := pool.Exec(ctx, finishParser, status, parserId); err != nil {
		logger.Error("unable to finish parser: %s", err)
	}
}

func consumeQueuedParseEvents(
	jobCounter *atomic.Uint32,
	ctx context.Context,
	pool *pgxpool.Pool,
) {
	jobCounter.Add(1)
	defer jobCounter.Store(jobCounter.Load() - 1)

	const consume = `
		update parser set
			status = 'pe_in_progress',
			started_at = now()
		where
			id = (
				select p.id from parser p where
					p.status = 'pe_queued'
				order by
					p.queued_at asc
				limit
					1
			)
		returning
			id, user_account_id
	`
	var parserId, userAccountId int32
	err := pool.QueryRow(ctx, consume).Scan(&parserId, &userAccountId)
	if errors.Is(err, pgx.ErrNoRows) {
		return
	}
	if err != nil {
		logger.Error("unable to query: %s", err)
		return
	}

	var eg errgroup.Group
	groupCtx, groupCtxCancel := context.WithCancel(ctx)

	// parse
	eg.Go(func() error {
		defer groupCtxCancel()
		return parser.ParseEvents(groupCtx, pool, userAccountId)
	})

	// TODO:
	// var errPTReset = errors.New("pt_reset")
	// var errPEReset = errors.New("pe_reset")
	//
	// listen for reset
	eg.Go(func() error {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			<-ticker.C

			const selectReset = `
				select status from parser where
					id = $1 and
					status = 'reset'
			`
			var status db.ParserStatus
			err := pool.QueryRow(groupCtx, selectReset, parserId).Scan(&status)
			if errors.Is(err, context.Canceled) {
				return nil
			}
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			if err != nil {
				logger.Error("unable to select reset parser: %s", err)
				continue
			}

			groupCtxCancel()
			logger.Info("parser reset")

			// switch status {
			// }

			return groupCtx.Err()
		}
	})

	parserErr := eg.Wait()

	var status db.ParserStatus

	switch {
	case parserErr == nil:
		status = db.ParserStatusSuccess
	case errors.Is(parserErr, context.Canceled):
		status = db.ParserStatusPTQueued
	default:
		status = db.ParserStatusPEError
	}

	const finishParser = `
		update parser set
			finished_at = now(),
			status = $1
		where
			id = $2
	`
	if _, err := pool.Exec(ctx, finishParser, status, parserId); err != nil {
		logger.Error("unable to finish parser: %s", err)
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
			go consumeQueuedParseTransactions(
				&runningParseTransactions,
				&runningParseEvents,
				context.TODO(), pool, evmClient,
			)
		}

		if runningParseEvents.Load() == 0 {
			go consumeQueuedParseEvents(
				&runningParseEvents,
				context.TODO(), pool,
			)
		}
	}
}
