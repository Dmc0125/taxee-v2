package parser

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"
	"taxee/pkg/assert"
	"taxee/pkg/coingecko"
	"taxee/pkg/db"
	"taxee/pkg/logger"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

func newDecimalFromRawAmount(amount uint64, decimals uint8) decimal.Decimal {
	d := decimal.NewFromBigInt(
		new(big.Int).SetUint64(amount),
		-int32(decimals),
	)
	return d
}

type parserError interface {
	IsEq(parserError) bool
	Address() string
}

type parserErrors map[string][]parserError

func (errors parserErrors) appendErr(
	err parserError,
) {
	addr := err.Address()
	accountErrors := errors[addr]

	switch {
	case len(accountErrors) == 0:
		accountErrors = append(accountErrors, err)
	default:
		last := accountErrors[len(accountErrors)-1]
		if !last.IsEq(err) {
			accountErrors = append(accountErrors, err)
		}
	}

	errors[addr] = accountErrors
}

func devDeleteParsed(
	ctx context.Context,
	pool *pgxpool.Pool,
	userAccountId int32,
) error {
	const q = "call dev_delete_parsed($1)"
	_, err := pool.Exec(ctx, q, userAccountId)
	if err != nil {
		return fmt.Errorf("unable to call dev_delete_parsed: %w", err)
	}
	return nil
}

func Parse(
	ctx context.Context,
	pool *pgxpool.Pool,
	userAccountId int32,
	fresh bool,
) {
	if os.Getenv("MEM_PROF") == "1" {
		var wg sync.WaitGroup
		wg.Add(1)
		defer wg.Wait()

		go func() {
			defer wg.Done()
			fmt.Println("Pprof available at: http://localhost:6060/debug/pprof")
			err := http.ListenAndServe("localhost:6060", nil)
			assert.NoErr(err, "")
		}()
	}

	if fresh {
		err := devDeleteParsed(ctx, pool, userAccountId)
		assert.NoErr(err, "")
	}

	wallets, err := db.GetWallets(ctx, pool, userAccountId)
	assert.NoErr(err, "")

	solanaWallets := make([]string, 0)

	for _, w := range wallets {
		switch w.Network {
		case db.NetworkSolana:
			solanaWallets = append(solanaWallets, w.Address)
		}
	}

	// TODO:
	// pagination ??
	// 700 txs ~= 6.4mb of allocations

	txs, err := db.GetTransactions(ctx, pool, userAccountId)
	assert.NoErr(err, "")

	///////////////////
	// preprocess txs
	errs := make(parserErrors)
	solanaAccounts := make(map[string][]accountLifetime)

	for _, tx := range txs {
		if tx.Err {
			continue
		}

		switch txData := tx.Data.(type) {
		case *db.SolanaTransactionData:
			solPreprocessTx(
				errs,
				solanaAccounts,
				solanaWallets,
				tx.Id,
				txData,
			)
		}
	}

	var batch pgx.Batch
	// NOTE: Errors are created sequentially based on txs, so they are
	// always created in the correct order, so just keeping global
	// position is fine
	errPos := int32(0)
	for _, errs := range errs {
		for _, err := range errs {
			txId, address, ixIdx := "", "", int32(0)
			var (
				kind db.ErrType
				data []byte
			)

			switch e := err.(type) {
			case *errAccountMissing:
				txId, address = e.txId, e.address
				ixIdx = int32(e.ixIdx)
				kind = db.ErrTypeAccountMissing
			case *errAccountBalanceMismatch:
				txId, address = e.txId, e.address
				ixIdx = int32(e.ixIdx)
				kind = db.ErrTypeAccountBalanceMismatch

				var mErr error
				data, mErr = json.Marshal(db.ErrAccountBalanceMismatch{
					Expected: e.expected,
					Had:      e.had,
				})
				assert.NoErr(mErr, "unable to serialize ErrAccountBalanceMismatch")
			}

			q := db.EnqueueInsertErr(
				&batch,
				userAccountId,
				txId,
				pgtype.Int4{Int32: ixIdx, Valid: true},
				errPos,
				db.ErrOriginPreprocess,
				kind,
				address,
				data,
			)
			q.Exec(func(_ pgconn.CommandTag) error { return nil })
			errPos += 1
		}
	}

	br := pool.SendBatch(ctx, &batch)
	err = br.Close()
	assert.NoErr(err, "unable to insert errors")

	///////////////////
	// process txs

	events := make([]*db.Event, 0)

	for _, tx := range txs {
		if tx.Err {
			continue
		}

		switch txData := tx.Data.(type) {
		case *db.SolanaTransactionData:
			solanaCtx := solanaContext{
				wallets:        solanaWallets,
				accounts:       solanaAccounts,
				parserErrors:   errs,
				txId:           tx.Id,
				slot:           txData.Slot,
				timestamp:      tx.Timestamp,
				tokensDecimals: txData.TokenDecimals,
			}

			for ixIdx, ix := range txData.Instructions {
				solanaCtx.ixIdx = uint32(ixIdx)
				solProcessIx(&events, &solanaCtx, ix)
			}
		}
	}

	queryPricesBatch := pgx.Batch{}
	type tokenPriceToFetch struct {
		coingeckoId string
		timestamp   int64
		eventIdx    int
	}
	tokenPricesToFetch := make([]tokenPriceToFetch, 0)

	for eventIdx, event := range events {
		var token string

		switch data := event.Data.(type) {
		case *db.EventTransferInternal:
			token = data.Token
		case *db.EventTransfer:
			token = data.Token
		default:
			assert.True(false, "unknown event data: %T %#v", data, *event)
			continue
		}

		const hour, halfHour = 60 * 60, 60 * 30
		roundedTimestamp := time.Unix(
			(event.Timestamp.Unix()+halfHour)/int64(hour)*int64(hour),
			0,
		)

		q := db.EnqueueGetPricepoint(
			&queryPricesBatch,
			event.Network,
			token,
			roundedTimestamp,
		)
		q.QueryRow(func(row pgx.Row) error {
			price, coingeckoId, ok, err := db.QueueScanPricepoint(row)
			// TODO: coingecko may not have the information about the token
			if errors.Is(err, pgx.ErrNoRows) {
				// TODO: Error
				// NOTE: coingecko does not have the token data
				return nil
			} else {
				assert.NoErr(err, fmt.Sprintf("unable to scan pricepoint: %s", token))
			}

			if ok {
				logger.Info("Found pricepoint in db: %s %s", coingeckoId, roundedTimestamp)
				event.Data.SetPrice(price)
			} else {
				tokenPricesToFetch = append(
					tokenPricesToFetch,
					tokenPriceToFetch{
						coingeckoId,
						roundedTimestamp.Unix(),
						eventIdx,
					},
				)
			}
			return nil
		})
	}

	br = pool.SendBatch(ctx, &queryPricesBatch)
	err = br.Close()
	assert.NoErr(err, "unable to query prices")

	type coingeckoTokenPriceId struct {
		coingeckoId string
		timestamp   int64
	}
	coingeckoTokensPrices := make(map[coingeckoTokenPriceId]decimal.Decimal)

	for _, token := range tokenPricesToFetch {
		ts := time.Unix(token.timestamp, 0)
		price, ok := coingeckoTokensPrices[coingeckoTokenPriceId{
			token.coingeckoId, token.timestamp,
		}]

		if !ok {
			coingeckoPrices, err := coingecko.GetCoinOhlc(
				token.coingeckoId,
				coingecko.FiatCurrencyEur,
				ts,
			)
			assert.NoErr(err, "unable to get coingecko prices")

			if len(coingeckoPrices) > 0 {
				insertPricesBatch := pgx.Batch{}
				for _, p := range coingeckoPrices {
					q := db.EnqueueInsertPricepoint(
						&insertPricesBatch,
						p.Close,
						p.Timestamp,
						token.coingeckoId,
					)
					q.Exec(func(ct pgconn.CommandTag) error { return nil })

					id := coingeckoTokenPriceId{token.coingeckoId, p.Timestamp.Unix()}
					coingeckoTokensPrices[id] = p.Close
				}
				br := pool.SendBatch(ctx, &insertPricesBatch)
				err := br.Close()
				assert.NoErr(err, "unable to insert coingecko prices")

			}
			if p := coingeckoPrices[0]; p.Timestamp.Equal(ts) {
				logger.Info("Fetched pricepoint from coingecko: %s %s", token.coingeckoId, ts)
				price, ok = p.Close, true
			}
		} else {
			logger.Info("Found pricepoint in coingecko prices: %s %s", token.coingeckoId, ts)
		}

		event := events[token.eventIdx]
		event.Data.SetPrice(price)
	}

	inv := inventory{
		accounts: make(map[inventoryAccountId][]*inventoryAccount),
	}
	eventsBatch := pgx.Batch{}
	for eventIdx, event := range events {
		inv.processEvent(event)

		_, err := db.EnqueueInsertEvent(
			&eventsBatch,
			userAccountId,
			int32(eventIdx),
			event,
		)
		assert.NoErr(err, "")
	}

	br = pool.SendBatch(ctx, &eventsBatch)
	err = br.Close()
	assert.NoErr(err, "unable to insert events")
}
