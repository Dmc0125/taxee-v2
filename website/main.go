package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path"
	"runtime"
	"slices"
	"strings"
	"taxee/pkg/assert"
	"taxee/pkg/db"
	"taxee/pkg/dotenv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type componentHandle struct {
	data     []byte
	params   map[string]string
	children []byte
	rendered bool
}

func (handle *componentHandle) setParam(key, value string) {
	_ = "bg-red-500"
	handle.params[key] = value
}

func (handle *componentHandle) addChild(comp *componentHandle) {
	if !comp.rendered {
		comp.render()
	}

	handle.children = append(handle.children, comp.data...)
}

func (handle *componentHandle) render() {
	for i := 0; i < len(handle.data); i += 1 {
		c := handle.data[i]

		switch c {
		case '{':
			start := i

			if handle.data[i+1] != '{' {
				continue
			}
			// skip over {{
			i += 2
			ident := make([]byte, 0)

			if handle.data[i] == '}' && handle.data[i+1] == '}' {
				// skip over }}
				i += 2
				continue
			}

			for {
				c = handle.data[i]
				ident = append(ident, c)

				next1, next2 := handle.data[i+1], handle.data[i+2]
				if next1 == '}' && next2 == '}' {
					break
				}

				i += 1
			}
			// skip over current and }, next } skips because of loop
			i += 2
			end := i + 1
			n := string(ident)

			if n == "children" {
				handle.data = slices.Replace(handle.data, start, end, handle.children...)
			} else {
				p, ok := handle.params[n]
				assert.True(ok, "param \"%s\" missing", n)

				handle.data = slices.Replace(handle.data, start, end, []byte(p)...)
			}

			ident = ident[:0]
		}
	}

	handle.rendered = true
}

var components = make(map[string]componentHandle)

func component(key string) componentHandle {
	comp, ok := components[key]
	assert.True(ok, "component %s missing", key)

	data := make([]byte, len(comp.data))
	copy(data, comp.data)

	return componentHandle{
		data:     data,
		params:   make(map[string]string),
		children: make([]byte, 0),
	}
}

func loadComponents(prod bool) {
	switch {
	case prod:
		componentsPath := os.Getenv("COMPONENTS_PATH")
		assert.True(componentsPath != "", "missing path to components")
		// TODO: validate the path
	default:
		_, file, _, ok := runtime.Caller(0)
		assert.True(ok, "unable to get runtime caller")

		componentsDir := path.Join(file, "../components")
		entries, err := os.ReadDir(componentsDir)
		assert.NoErr(err, "unable to read components dir")

		for _, e := range entries {
			// TODO: handle?
			assert.True(!e.IsDir(), "components dir contains directory")

			n := e.Name()
			p := path.Join(componentsDir, n)

			data, err := os.ReadFile(p)
			assert.NoErr(err, fmt.Sprintf("unable to read component: %s", p))

			ident := strings.Split(n, ".")
			components[ident[0]] = componentHandle{
				data: data,
			}
		}
	}
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

type transactionRow struct {
	Id        string
	Err       bool
	Fee       int64
	Signer    string
	Timestamp time.Time
	Data      *db.SolanaTransactionData
}

func getTransactions(
	pool *pgxpool.Pool,
	slot int64,
	blockIndex int32,
) (
	res []*transactionRow, lastSlot int64, lastBlockIndex int32, err error,
) {
	const query = `
		select
			id, err, fee, signer, timestamp, data
		from
			tx
		where
			(
				(data->>'slot')::bigint = $1 and
				(data->>'blockIndex')::integer > $2
			) or (data->>'slot')::bigint > $1
		order by
			(data->>'blockIndex')::integer asc,
			(data->>'slot')::bigint asc
		limit
			50
	`
	rows, qErr := pool.Query(context.Background(), query, slot, blockIndex)
	if qErr != nil {
		err = qErr
		return
	}

	res = make([]*transactionRow, 0)
	for rows.Next() {
		var tx transactionRow
		var dataMarshalled []byte
		err = rows.Scan(
			&tx.Id, &tx.Err, &tx.Fee, &tx.Signer, &tx.Timestamp, &dataMarshalled,
		)
		if err != nil {
			return
		}

		data := new(db.SolanaTransactionData)
		if err = json.Unmarshal(dataMarshalled, &data); err != nil {
			return
		}

		tx.Data = data
		res = append(res, &tx)
	}

	if len(res) > 0 {
		lastSlot = int64(res[len(res)-1].Data.Slot)
	}

	return
}

func main() {
	appEnv := os.Getenv("APP_ENV")
	prod := true
	if appEnv != "PROD" {
		assert.NoErr(dotenv.ReadEnv(), "")
		prod = false
	}

	pool, err := db.InitPool(context.Background(), appEnv)
	assert.NoErr(err, "")

	serveStaticFiles(prod)
	loadComponents(prod)

	prevSlot, prevBlockIndex := int64(-1), int32(-1)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var txs []*transactionRow
		var err error
		txs, prevSlot, prevBlockIndex, err = getTransactions(
			pool, prevSlot, prevBlockIndex,
		)
		if err != nil {
			w.WriteHeader(500)
			msg := fmt.Sprintf("500 server error: %s", err)
			w.Write([]byte(msg))
			return
		}

		txsComp := component("transactions")

		for _, tx := range txs {
			txComp := component("transaction")

			txIdShort := fmt.Sprintf("%s...%s", tx.Id[:4], tx.Id[len(tx.Id)-4:len(tx.Id)])
			txComp.setParam("txIdShort", txIdShort)
			txComp.setParam("txId", tx.Id)

			txsComp.addChild(&txComp)
		}

		wrapper := component("wrapper")
		wrapper.addChild(&txsComp)
		wrapper.render()

		w.Header().Set("content-type", "text/html")
		w.Write(wrapper.data)
	})

	fmt.Println("Listening at: http://localhost:8888")
	err = http.ListenAndServe("localhost:8888", nil)
	assert.NoErr(err, "")
}
