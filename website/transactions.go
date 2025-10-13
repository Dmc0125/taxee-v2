package main

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"taxee/pkg/assert"
	"taxee/pkg/db"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type transactionsPageComponentData struct {
	Id           string
	Timestamp    time.Time
	Network      db.Network
	Err          bool
	Instructions []*db.SolanaInstruction
}

const transactionsPageComponent = `
{{ define "transactions_page" }}
	<div class="mx-auto w-[80%] py-10">
		<header class="py-10 flex items-center justify-between">
			<h1 class="text-3xl font-semibold text-gray-900">Transactions</h1>
			<div>
				<a
					href="?from_slot=&from_block_index="
				>
					Next
				</a>
			</div>
		</header>

		<ul class="w-full flex flex-col gap-y-4">
			{{ range . }}
				<li class="w-full flex flex-col gap-y-4">
					<div class="
						flex items-center justify-between px-4 py-2
						border border-gray-300 rounded-lg
					">
						<a href="https://solscan.io/tx/{{ .Id }}" target="_blank">
							{{ shorten .Id 8 4 }}
						</a>
						<p class="text-sm">{{ formatDate .Timestamp }} {{formatTime .Timestamp }}</p>
					</div>
					<div class="px-4">
						{{ range $idx, $ix := .Instructions }}
							<div>
								{{ $idx }}
								{{ $ix.ProgramAddress }}
							</div>
						{{ end }}
					</div>
				</li>
			{{ end }}
		</ul>
	</div>
{{ end }}
`

func getTransactions(
	ctx context.Context,
	pool *pgxpool.Pool,
	userAccountId int32,
) ([]*transactionsPageComponentData, error) {
	const q = `
		select
			tx.id, tx.network, tx.err, tx.data, tx.timestamp
		from
			tx
		inner join
			tx_ref tr on
				tr.user_account_id = $1 and
				tr.tx_id = tx.id
		order by
			(tx.data->>'slot')::bigint asc,
			(tx.data->>'blockIndex')::integer asc
	`
	rows, err := pool.Query(ctx, q, userAccountId)
	if err != nil {
		return nil, fmt.Errorf("unable to query transactions: %w", err)
	}

	res := make([]*transactionsPageComponentData, 0)

	for rows.Next() {
		tx := new(transactionsPageComponentData)
		var dataMarshalled []byte
		err := rows.Scan(&tx.Id, &tx.Network, &tx.Err, &dataMarshalled, &tx.Timestamp)
		if err != nil {
			return nil, fmt.Errorf("unable to scan tx: %w", err)
		}

		switch tx.Network {
		case db.NetworkSolana:
			data := new(db.SolanaTransactionData)
			err := json.Unmarshal(dataMarshalled, &data)
			assert.NoErr(err, "unable to unmarshal solana tx data")
			tx.Instructions = data.Instructions
		}

		res = append(res, tx)
	}

	return res, nil
}

func transactionsHandler(
	ctx context.Context,
	pool *pgxpool.Pool,
	templates *template.Template,
) http.HandlerFunc {
	loadTemplate(templates, transactionsPageComponent)

	return func(w http.ResponseWriter, r *http.Request) {
		userAccountId := int32(1)

		txs, err := getTransactions(ctx, pool, userAccountId)
		if err != nil {
			renderError(w, 500, err.Error())
			return
		}

		transactionsRendered := executeTemplateMust(
			templates, "transactions_page", txs,
		)

		page := executeTemplateMust(templates, "page_layout", pageLayoutComponentData{
			Content: template.HTML(transactionsRendered),
		})
		w.Write(page)
		return
	}
}
