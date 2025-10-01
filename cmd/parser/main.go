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

func Parse(ctx context.Context, pool *pgxpool.Pool, userAccountId int32) {
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

	wallets, err := db.GetWallets(ctx, pool, userAccountId)
	assert.NoErr(err, "")

	solanaWallets := make([]string, 0)
	for _, w := range wallets {
		switch w.Network {
		case db.NetworkSolana:
			solanaWallets = append(solanaWallets, w.Address)
		}
	}

	var solanaCtx solana.Context
	solanaCtx.Init(solanaWallets)

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

		solanaCtx.TxId = tx.Id

		switch txData := tx.Data.(type) {
		case *db.SolanaTransactionData:
			solana.PreparseTx(&solanaCtx, txData)
		}
	}

	var batch pgx.Batch
	// NOTE: Errors are created sequentially based on txs, so they are
	// always created in the correct order, so just keeping global
	// position is fine
	errPos := int32(0)
	for _, errs := range solanaCtx.Errors {
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

	// for _, tx := range txs {
	//
	// }

}
