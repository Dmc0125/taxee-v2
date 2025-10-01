package main

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"taxee/pkg/assert"
	"taxee/pkg/db"
	"taxee/pkg/dotenv"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type parserErr struct {
	IxIdx   int32
	Kind    db.ErrType
	Address string
	Data    any
}

type TransactionCmpData struct {
	Id             string
	Timestamp      time.Time
	Err            bool
	PreprocessErrs []*parserErr
	ParserErrs     []*parserErr
}

var transactionCmp = `
{{define "transaction"}}
	<li class="
			bg-gray-50 rounded-lg border border-gray-300 shadow-xs
			py-2 px-4
		"
	>
		{{ $preprocessErrsLen := len .PreprocessErrs }}
		{{ $parserErrsLen := len .ParserErrs }}

		<div class="flex justify-between">
			<div class="flex gap-x-4 items-center">
				<a
					class="font-mono text-gray-900"
					href="https://solscan.io/tx/{{.Id}}"
					target="_blank"
					rel="noopener noreferrer"
				>{{shorten .Id 8 8}}</a>
				<p class="font-mono text-gray-600 text-sm">{{formatDate .Timestamp}}</p>
			</div>
			<div>
				{{if .Err}}
					<div class="
						bg-orange-200 border border-yellow-300 py-1 px-3 rounded-full
						text-yellow-600 text-xs font-semibold
					">
						Onchain error
					</div>
				{{end}}
				{{if ne 0 $preprocessErrsLen}}
					<div class="
						bg-pink-600 rounded-md py-1 px-2 
						text-gray-100 text-xs font-semibold
					">
						{{ $preprocessErrsLen }}
					</div>
				{{end}}
			</div>
		</div>

		{{if ne 0 $preprocessErrsLen}}
			<div class="mt-4">
				<h3 class="font-semibold text-gray-800">Preprocess Errors</h3>

				<div class="mt-2 flex flex-wrap gap-4">
					{{range .PreprocessErrs}}
						<div class="bg-red-100 border-red-300 border rounded-md py-2 px-4">
							<div class="flex gap-2">
								<p class="font-semibold text-sm text-gray-800">
									{{toTitle (printf "%s" .Kind)}}
								</p>
								{{if ne .IxIdx -1}}
									<p class="text-sm text-gray-700">Ix: {{ .IxIdx }}</p>
								{{end}}
							</div>
							<p class="font-mono text-gray-800">
								{{shorten .Address 4 4}}
							</p>
							{{if eq .Kind "account_balance_mismatch"}}
								<div>
									<h1 class="text-3xl font-bold">FIXME</h1>
									<p>Expected: {{index .Data "expected"}}</p>
									<p>Had: {{index .Data "had"}}</p>
								</div>
							{{end}}
						</div>
					{{end}}
				</div>
			</div>
		{{end}}
	</li>
{{end}}
`

type TransactionsCmpData struct {
	NextSlot       int64
	NextBlockIndex int32
	Transactions   []TransactionCmpData
}

var transactionsCmp = `
{{define "transactions"}}
	<div class="mx-auto w-[70%] py-10">
		<header class="py-10 flex items-center justify-between">
			<h1 class="text-3xl font-semibold text-gray-900">Transactions</h1>

			<div>
				<a
					href="?from_slot={{.NextSlot}}&from_block_index={{.NextBlockIndex}}"
				>
					Next
				</a>
			</div>
		</header>
		<ul class="flex flex-col gap-y-4">
			{{range .Transactions}}
				{{template "transaction" .}}
			{{end}}
		</ul>
	</div>
{{end}}
`

var transactionsPage = `
{{define "transactions_page"}}
	<!DOCTYPE html>
	<html>
		<head>
			<title>Taxee</title>
			<link href="static/output.css" rel="stylesheet">
		</head>
		<body class="min-h-screen bg-gray-50">
			{{template "transactions" .}}
		</body>
	</html>
{{end}}
`

func loadTemplate(templates *template.Template, tmpl string) {
	var err error
	templates, err = templates.Parse(tmpl)
	assert.NoErr(err, "unable to load template")
}

type bufWriter []byte

func (b *bufWriter) Write(n []byte) (int, error) {
	*b = append(*b, n...)
	return len(n), nil
}

func executeTemplate(tmpl *template.Template, w http.ResponseWriter, name string, data any) error {
	buf := bufWriter([]byte{})
	err := tmpl.ExecuteTemplate(&buf, name, data)
	if err != nil {
		return err
	}
	w.Write(buf)
	return nil
}

func serveStaticFiles(prod bool) {
	staticDir := ""

	switch {
	case prod:
		staticPath := os.Getenv("STATIC_PATH")
		assert.True(staticPath != "", "missing path to static files")
	default:
		_, file, _, ok := runtime.Caller(0)
		assert.True(ok, "unable to get runtime caller")
		staticDir = path.Join(file, "../static")
	}

	handler := http.FileServer(http.Dir(staticDir))
	http.Handle("/static/", http.StripPrefix("/static/", handler))
}

type getTxsWithParserErrsPaginatedRowParserErr struct {
	IxIdx   pgtype.Int4
	Idx     int32
	Origin  db.ErrOrigin
	Kind    db.ErrType
	Address string
	Data    []byte
}

type getTxsWithParserErrsPaginatedRow struct {
	Id         string
	Err        bool
	Fee        int64
	Signer     string
	Timestamp  time.Time
	Data       *db.SolanaTransactionData
	ParserErrs pgtype.FlatArray[*getTxsWithParserErrsPaginatedRowParserErr]
}

func getTxsWithParserErrsPaginated(
	pool *pgxpool.Pool,
	userAccountId int32,
	slot int64,
	blockIndex int32,
	limit int32,
) (
	[]*getTxsWithParserErrsPaginatedRow,
	int64,
	int32,
	error,
) {
	const query = `
		select
			tx.id, tx.err, tx.fee, tx.signer, tx.timestamp, tx.data,
			array_agg(
				row(
					err.ix_idx,
					err.idx,
					err.origin,
					err.type,
					err.address,
					err.data
				) order by err.idx asc
			) filter (where err.id is not null) as errs
		from
			tx
		inner join
			tx_ref on tx_ref.tx_id = tx.id and tx_ref.user_account_id = $1
		left join
			err on err.tx_id = tx.id
		where
			(
				(tx.data->>'slot')::bigint = $2 and
				(tx.data->>'blockIndex')::integer > $3 
			) or 
			(tx.data->>'slot')::bigint > $2
		group by
			tx.id
		order by
			(tx.data->>'blockIndex')::integer asc,
			(tx.data->>'slot')::bigint asc
		limit
			$4
	`
	rows, err := pool.Query(
		context.Background(), query, userAccountId, slot, blockIndex, limit,
	)
	if err != nil {
		return nil, 0, 0, err
	}

	res := make([]*getTxsWithParserErrsPaginatedRow, 0)
	for rows.Next() {
		var tx getTxsWithParserErrsPaginatedRow
		var dataMarshalled []byte
		err = rows.Scan(
			&tx.Id, &tx.Err, &tx.Fee, &tx.Signer, &tx.Timestamp, &dataMarshalled, &tx.ParserErrs,
		)
		if err != nil {
			return nil, 0, 0, err
		}

		data := new(db.SolanaTransactionData)
		if err = json.Unmarshal(dataMarshalled, &data); err != nil {
			return nil, 0, 0, err
		}

		tx.Data = data
		res = append(res, &tx)
	}

	if err = rows.Err(); err != nil {
		return nil, 0, 0, err
	}

	lastSlot, lastBlockIndex := int64(-1), int32(-1)

	if len(res) > 0 {
		lastSlot = int64(res[len(res)-1].Data.Slot)
		lastBlockIndex = int32(res[len(res)-1].Data.BlockIndex)
	}

	return res, lastSlot, lastBlockIndex, nil
}

func main() {
	appEnv := os.Getenv("APP_ENV")
	prod := true
	if appEnv != "PROD" {
		assert.NoErr(dotenv.ReadEnv(), "")
		prod = false
	}

	templateFuncs := template.FuncMap{
		"formatDate": func(t time.Time) string {
			return t.Format("02/01/2006 15:04:05")
		},
		"shorten": func(s string, start, end int) string {
			return fmt.Sprintf("%s...%s", s[:start], s[len(s)-end:])
		},
		"toTitle": func(s string) string {
			const toLower = 32
			t := strings.Builder{}

			isLower := func(c byte) bool {
				return (c >= 97 && c <= 122)
			}

			if isLower(s[0]) {
				t.WriteByte(s[0] - toLower)
			} else {
				t.WriteByte(s[0])
			}

			isAlpha := func(c byte) bool {
				return isLower(c) || 65 <= c && c <= 90
			}

			for i := 1; i < len(s); i += 1 {
				prev := s[i-1]
				c := s[i]

				switch {
				case c == 95:
					t.WriteByte(32)
				case !isAlpha(prev) && isLower(c):
					t.WriteByte(c - toLower)
				default:
					t.WriteByte(c)
				}
			}

			return t.String()
		},
	}
	var templates *template.Template = template.New("root").Funcs(templateFuncs)
	loadTemplate(templates, transactionsPage)
	loadTemplate(templates, transactionsCmp)
	loadTemplate(templates, transactionCmp)

	pool, err := db.InitPool(context.Background(), appEnv)
	assert.NoErr(err, "")

	serveStaticFiles(prod)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()

		userAccountId := int32(1)
		if prod {
			userAccountIdParam := query.Get("user_account_id")
			id, err := strconv.Atoi(userAccountIdParam)
			if err != nil {
				w.WriteHeader(400)
				w.Write(fmt.Appendf(nil, "Invalid user account id: %s", userAccountIdParam))
				return
			}
			userAccountId = int32(id)

			// TODO: Auth
		}

		fromSlotParam, fromBlockIndexParam := query.Get("from_slot"), query.Get("from_block_index")
		fromSlot, err := strconv.Atoi(fromSlotParam)
		if err != nil {
			fromSlot = -1
		}
		fromBlockIndex, err := strconv.Atoi(fromBlockIndexParam)
		if err != nil {
			fromBlockIndex = -1
		}

		const LIMIT = 50
		var (
			txs            []*getTxsWithParserErrsPaginatedRow
			nextSlot       int64
			nextBlockIndex int32
		)
		txs, nextSlot, nextBlockIndex, err = getTxsWithParserErrsPaginated(
			pool, userAccountId, int64(fromSlot), int32(fromBlockIndex), LIMIT,
		)
		if err != nil {
			w.WriteHeader(500)
			msg := fmt.Sprintf("500 server error: %s", err)
			w.Write([]byte(msg))
			return
		}

		txsCompData := TransactionsCmpData{
			NextSlot:       nextSlot,
			NextBlockIndex: nextBlockIndex,
			Transactions:   make([]TransactionCmpData, len(txs)),
		}

		for i, tx := range txs {
			txsCompData.Transactions[i].Id = tx.Id
			txsCompData.Transactions[i].Timestamp = tx.Timestamp
			txsCompData.Transactions[i].Err = tx.Err

			preprocessErrs := make([]*parserErr, 0)
			parserErrs := make([]*parserErr, 0)

			for _, err := range tx.ParserErrs {
				var data any = struct{}{}
				switch err.Kind {
				case db.ErrTypeAccountBalanceMismatch:
					err := json.Unmarshal(err.Data, &data)
					assert.NoErr(err, "unable to unmarshal")
				case db.ErrTypeAccountMissing:
				}

				e := &parserErr{
					IxIdx:   -1,
					Kind:    err.Kind,
					Address: err.Address,
					Data:    data,
				}

				if err.IxIdx.Valid {
					e.IxIdx = err.IxIdx.Int32
				}

				switch err.Origin {
				case db.ErrOriginPreprocess:
					preprocessErrs = append(preprocessErrs, e)
				case db.ErrOriginParse:
					parserErrs = append(parserErrs, e)
				}
			}

			txsCompData.Transactions[i].PreprocessErrs = preprocessErrs
			txsCompData.Transactions[i].ParserErrs = parserErrs
		}

		w.Header().Set("content-type", "text/html")
		err = executeTemplate(templates, w, "transactions_page", txsCompData)
		if err != nil {
			w.WriteHeader(500)
			m := fmt.Sprintf("500 server error: %s", err)
			w.Write([]byte(m))
		}
	})

	fmt.Println("Listening at: http://localhost:8888")
	err = http.ListenAndServe("localhost:8888", nil)
	assert.NoErr(err, "")
}
