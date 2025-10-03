package parser

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"
	"taxee/cmd/parser/solana"
	"taxee/pkg/assert"
	"taxee/pkg/db"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type parserError interface {
	IsEqual(other any) bool
}

type globalContext struct {
	wallets []*db.WalletRow
	errors  map[string][]parserError
}

func (ctx *globalContext) AppendErr(account string, err any) {
	e, ok := err.(parserError)
	assert.True(ok, "invalid err: %T", err)
	accountErrs := ctx.errors[account]

	switch {
	case len(accountErrs) == 0:
		accountErrs = append(accountErrs, e)
	default:
		last := accountErrs[len(accountErrs)-1]
		if !last.IsEqual(err) {
			accountErrs = append(accountErrs, e)
		}
	}

	ctx.errors[account] = accountErrs
}

func (ctx *globalContext) WalletOwned(network db.Network, address string) bool {
	for _, w := range ctx.wallets {
		if w.Network == network && w.Address == address {
			return true
		}
	}
	return false
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

type inventoryAccountId struct {
	address string
	network db.Network
	token   string
}

type inventory struct {
}

func (inv *inventory) processEvent(event db.Event) {}

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

	globalCtx := globalContext{
		wallets: wallets,
		errors:  make(map[string][]parserError),
	}
	solanaAccounts := make(map[string][]solana.AccountLifetime)

	// TODO:
	// pagination ??
	// 700 txs ~= 6.4mb of allocations

	txs, err := db.GetTransactions(ctx, pool, userAccountId)
	assert.NoErr(err, "")

	///////////////////
	// preprocess txs

	for _, tx := range txs {
		if tx.Err {
			continue
		}

		switch txData := tx.Data.(type) {
		case *db.SolanaTransactionData:
			solana.PreprocessTx(&globalCtx, solanaAccounts, tx.Id, txData)
		}
	}

	var batch pgx.Batch
	// NOTE: Errors are created sequentially based on txs, so they are
	// always created in the correct order, so just keeping global
	// position is fine
	errPos := int32(0)
	for _, errs := range globalCtx.errors {
		for _, err := range errs {
			txId, address, ixIdx := "", "", int32(0)
			var (
				kind db.ErrType
				data []byte
			)

			switch e := err.(type) {
			case *solana.ErrAccountMissing:
				txId, address = e.TxId, e.Address
				ixIdx = int32(e.IxIdx)
				kind = db.ErrTypeAccountMissing
			case *solana.ErrAccountBalanceMismatch:
				txId, address = e.TxId, e.Address
				ixIdx = int32(e.IxIdx)
				kind = db.ErrTypeAccountBalanceMismatch

				var mErr error
				data, mErr = json.Marshal(db.ErrAccountBalanceMismatch{
					Expected: e.Expected,
					Had:      e.Had,
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
	inv := inventory{}
	batch = pgx.Batch{}

	for _, tx := range txs {
		switch txData := tx.Data.(type) {
		case *db.SolanaTransactionData:
			if tx.Err {
				// only fee
				continue
			}

			for ixIdx, ix := range txData.Instructions {
				events := make([]db.Event, 0)
				solana.ProcessIx(
					&globalCtx,
					solanaAccounts,
					&events,
					tx.Id,
					txData.Slot,
					txData.TokenDecimals,
					uint32(ixIdx),
					ix,
				)

				for eventIdx, event := range events {
					inv.processEvent(event)

					q := db.EnqueueInsertUserEvent(
						&batch,
						userAccountId,
						tx.Id,
						int32(ixIdx),
						int32(eventIdx),
						event,
					)
					q.Exec(func(_ pgconn.CommandTag) error { return nil })
				}
			}

			// validate balances
		}
	}

	br = pool.SendBatch(ctx, &batch)
	err = br.Close()
	assert.NoErr(err, "unable to insert events")
}
