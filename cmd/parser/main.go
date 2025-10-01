package parser

import (
	"context"
	"database/sql"
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
	"github.com/jackc/pgx/v5/pgxpool"
)

func Parse(ctx context.Context, pool *pgxpool.Pool) {
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

	iterationId, err := db.InsertIteration(ctx, pool)
	assert.NoErr(err, "")

	wallets, err := db.GetWallets(ctx, pool)
	assert.NoErr(err, "")

	walletsIds := make([]int32, len(wallets))
	solanaWallets := make([]string, 0)
	for i, w := range wallets {
		walletsIds[i] = w.Id

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

	txs, err := db.GetTransactions(ctx, pool, walletsIds)
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
	for _, errs := range solanaCtx.Errors {
		for idx, err := range errs {
			txId, address := "", ""
			var (
				kind  db.ErrType
				data  db.NullJson
				ixIdx sql.NullInt32
			)

			switch e := err.(type) {
			case *solana.ErrAccountMissing:
				txId, address, ixIdx.Int32 = e.TxId, e.Address, int32(e.IxIdx)
				kind = db.ErrTypeAccountMissing
			case *solana.ErrAccountBalanceMissing:
				txId, address, ixIdx.Int32 = e.TxId, e.Address, int32(e.IxIdx)
				kind = db.ErrTypeAccountBalanceMismatch

				var mErr error
				data.Data, mErr = json.Marshal(db.ErrAccountBalanceMissing{
					Expected: e.Expected,
					Had:      e.Had,
				})
				assert.NoErr(mErr, "unable to serialize ErrAccountBalanceMismatch")
				data.Valid = true
			}

			q := db.EnqueueInsertErr(
				&batch,
				iterationId,
				txId,
				ixIdx,
				int32(idx),
				db.ErrOriginPreprocess,
				kind,
				address,
				data,
			)
			q.Exec(func(_ pgconn.CommandTag) error { return nil })
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
