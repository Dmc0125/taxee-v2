package parser

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"
	"taxee/cmd/parser/solana"
	"taxee/pkg/assert"
	"taxee/pkg/db"

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

	///////////////////
	// process txs

	// for _, tx := range txs {
	//
	// }

}
