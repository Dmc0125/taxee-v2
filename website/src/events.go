package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"math"
	"net/http"
	"taxee/pkg/assert"
	"taxee/pkg/db"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

type response struct {
	status int
	data   []byte
	err    error
}

func (r *response) write(w http.ResponseWriter) {
	if r.status == 0 {
		r.status = 500
	}
	w.WriteHeader(r.status)

	if r.status >= 200 && r.status < 300 {
		slog.Info("handler success", "status", r.status)
		w.Write(r.data)
	} else {
		slog.Error("handler err", "status", r.status, "error", r.err.Error())
		w.Write(r.data)
	}
}

func eventsHandler(pool *pgxpool.Pool, templates *template.Template) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var res response

		switch r.Method {
		case http.MethodGet:
			res = eventsGetHandler(pool, templates, r)
		default:
			res.status = http.StatusMethodNotAllowed
		}

		res.write(w)
	}
}

type cmpTransfer struct {
	Account     string
	ImageUrl    string
	CoingeckoId string

	Amount        decimal.Decimal
	Token         string
	Value         decimal.Decimal
	Price         decimal.Decimal
	Currency      string
	MissingAmount decimal.Decimal
}

type cmpEventMissingAmount struct {
	Amount  decimal.Decimal
	Account string
	Token   string
}

type cmpEventTransfers struct {
	WalletLabel string
	Transfers   []*cmpTransfer
}

type cmpEventProfit struct {
	Value    decimal.Decimal
	Currency string
}

type cmpEvent struct {
	Header          bool
	ShowGroupHeader bool
	Timestamp       time.Time
	TxId            pgtype.Text
	TxExplorerUrl   string

	App    string
	Method string

	OutgoingTransfers *cmpEventTransfers
	IncomingTransfers *cmpEventTransfers
	MissingAmounts    bool

	Profit *cmpEventProfit
}

func (c *cmpEvent) init(network db.Network) {
	if !c.TxId.Valid {
		return
	}

	g, ok := networksGlobals[network]
	assert.True(ok, "missing global for network: %d", network)
	c.TxExplorerUrl = fmt.Sprintf("%s/tx/%s", g.explorerUrl, c.TxId.String)
}

type tokenIdentifier struct {
	token       string
	network     db.Network
	tokenSource uint16
}

type tokenReference struct {
	coingeckoId *string
	token       *string
	imageUrl    *string
}

func tokensBatchAppend(
	b map[tokenIdentifier][]tokenReference,
	address string,
	network db.Network,
	tokenSource uint16,
	coingeckoId, token, logoUrl *string,
) {
	id := tokenIdentifier{address, network, tokenSource}
	b[id] = append(b[id], tokenReference{
		coingeckoId, token, logoUrl,
	})
}

func tokensBatchProcess(b map[tokenIdentifier][]tokenReference) *pgx.Batch {
	handleRow := func(row pgx.Row, token string, refs []tokenReference) error {
		var symbol, coingeckoId string
		var imageUrl pgtype.Text
		err := row.Scan(&symbol, &imageUrl, &coingeckoId)
		if errors.Is(err, pgx.ErrNoRows) {
			for _, tr := range refs {
				*tr.token = token
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("unable to query symbol: %w", err)
		}
		for _, tr := range refs {
			*tr.coingeckoId = coingeckoId
			*tr.token = symbol
			*tr.imageUrl = imageUrl.String
		}
		return nil
	}

	var batch pgx.Batch

	for tid, trefs := range b {
		if tid.tokenSource == math.MaxUint16 {
			const selectSymbol = `
				select symbol, image_url, coingecko_id from coingecko_token_data where coingecko_id = $1
			`
			batch.Queue(selectSymbol, tid.token).QueryRow(func(row pgx.Row) error {
				return handleRow(row, tid.token, trefs)
			})
		} else {
			const selectSymbol = `
				select ctd.symbol, ctd.image_url, ctd.coingecko_id from
					coingecko_token ct
				join
					coingecko_token_data ctd on ctd.coingecko_id = ct.coingecko_id
				where
					ct.network = $1 and ct.address = $2
			`
			batch.Queue(selectSymbol, tid.network, tid.token).QueryRow(func(row pgx.Row) error {
				return handleRow(row, tid.token, trefs)
			})
		}
	}

	return &batch
}

func (c *cmpEvent) appendTransfers(
	t *db.EventTransfer,
	labels map[string]string,
	network db.Network,
	tokensBatch map[tokenIdentifier][]tokenReference,
) {
	transferComponent := &cmpTransfer{
		Amount:   t.Amount,
		Value:    t.Value,
		Currency: "EUR",
	}

	tokensBatchAppend(
		tokensBatch,
		t.Token, network, t.TokenSource,
		&transferComponent.CoingeckoId, &transferComponent.Token, &transferComponent.ImageUrl,
	)

	if t.Direction == db.EventTransferIncoming || t.Direction == db.EventTransferInternal {
		if c.IncomingTransfers == nil {
			c.IncomingTransfers = new(cmpEventTransfers)
			c.IncomingTransfers.WalletLabel = labels[t.ToWallet]
		}

		transferComponent.Account = t.ToAccount
		c.IncomingTransfers.Transfers = append(c.IncomingTransfers.Transfers, transferComponent)
	}
	if t.Direction == db.EventTransferOutgoing || t.Direction == db.EventTransferInternal {
		if c.OutgoingTransfers == nil {
			c.OutgoingTransfers = new(cmpEventTransfers)
			c.OutgoingTransfers.WalletLabel = labels[t.FromWallet]
		}

		transferComponent.Account = t.FromAccount
		c.OutgoingTransfers.Transfers = append(c.OutgoingTransfers.Transfers, transferComponent)

		if t.MissingAmount.GreaterThan(decimal.Zero) {
			c.MissingAmounts = true
			transferComponent.MissingAmount = t.MissingAmount
		}
	}
}

type cmpEventsPage struct {
	Events  []*cmpEvent
	PrevUrl string
	NextUrl string
	UiPage  int
}

func eventsGetHandler(
	pool *pgxpool.Pool,
	templates *template.Template,
	r *http.Request,
) response {
	userAccountId := int32(1)

	tx, err := pool.BeginTx(r.Context(), pgx.TxOptions{
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return response{status: 500, err: err}
	}
	defer tx.Rollback(r.Context())

	// parser status

	const selectParser = `
		select status from parser where user_account_id = $1
	`
	var parserStatus db.ParserStatus
	err = tx.QueryRow(r.Context(), selectParser, userAccountId).Scan(&parserStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		parserStatus = db.ParserStatusUninitialized
	} else if err != nil {
		return response{status: 500, err: err}
	}

	navbarStatus := cmpNavbarStatus{}
	navbarStatus.appendParser(parserStatus)
	walletsLabels := make(map[string]string)

	// wallets

	const selectWallets = `
		select 
			status, label, address
		from
			wallet
		where
			user_account_id = $1 and status != 'delete'
	`
	walletsRows, err := tx.Query(r.Context(), selectWallets, userAccountId)
	if err != nil {
		return response{status: 500, err: err}
	}
	for walletsRows.Next() {
		var status db.WalletStatus
		var label, address string
		if err := walletsRows.Scan(&status, &label, &address); err != nil {
			return response{status: 500, err: err}
		}
		navbarStatus.appendWallet(label, status)
		walletsLabels[address] = label
	}

	var page int
	if err := parseIntParam(r.URL.Query(), &page, "page", true, 32); err != nil {
		if !errors.Is(err, errParamMissing) {
			return response{status: 400, err: err}
		}
	}

	var eventsHtml []byte

	switch parserStatus {
	case db.ParserStatusPTQueued, db.ParserStatusPTInProgress, db.ParserStatusPTError, db.ParserStatusUninitialized:
		// empty
		eventsHtml = executeTemplateMust(templates, "events_page", nil)
	// case db.ParserStatusSuccess:
	// 	// everything ok
	default:
		// transactions are parsed, events exist, but are not parsed
		const selectInternalTxs = `
			select
				itx.tx_id,
				itx.network,
				itx.timestamp,
				itx.position = max (itx.position) over (),
				coalesce(events.events, '[]'::json)
			from
				internal_tx itx
			left join lateral (
				select json_agg(json_build_object(
					'App', e.app,
					'Method', e.method,
					'Type', e.type,
					'Transfers', (
						select json_agg(json_build_object(
							'direction', et.direction,
							'fromWallet', et.from_wallet,
							'fromAccount', et.from_account,
							'toWallet', et.to_wallet,
							'toAccount', et.to_account,
							'token', et.token,
							'amount', et.amount,
							'tokenSource', et.token_source,
							'price', et.price,
							'value', et.value,
							'profit', et.profit,
							'missingAmount', et.missing_amount,
							'sources', (
								select json_agg(json_build_object(
									'transferId', ets.source_transfer_id,
									'usedAmount', ets.used_amount
								))
								from
									event_transfer_source ets
								where
									ets.transfer_id = et.id
							)
						) order by et.position asc)
						from
							event_transfer et
						where
							et.event_id = e.id
					)
				) order by e.position asc) as events
				from
					event e
				where 
					e.internal_tx_id = itx.id
			) events on true
			where
				itx.user_account_id = $1 and
				itx.position > $2
			order by
				position asc
			limit 
				$3
		`
		const limit = 20
		startPosition := limit * page
		internalTxsRows, err := tx.Query(
			r.Context(), selectInternalTxs,
			userAccountId, startPosition, limit,
		)
		if err != nil {
			return response{status: 500, err: err}
		}

		type eventData struct {
			App       string
			Method    string
			Type      db.EventType
			Transfers []*db.EventTransfer
		}

		var eventsComponents []*cmpEvent
		var isLast bool
		var lastDate time.Time
		tokensBatch := make(map[tokenIdentifier][]tokenReference)

		for internalTxsRows.Next() {
			var txId pgtype.Text
			var timestamp time.Time
			var network db.Network
			var eventsSerialized json.RawMessage
			if err := internalTxsRows.Scan(
				&txId, &network, &timestamp, &isLast, &eventsSerialized,
			); err != nil {
				return response{status: 500, err: err}
			}

			var events []*eventData
			if err := json.Unmarshal(eventsSerialized, &events); err != nil {
				return response{status: 500, err: err}
			}

			const day, halfday = 24 * 60 * 60, 12 * 60 * 60
			date := time.Unix((timestamp.Unix()+halfday)/day*day, 0)
			if !lastDate.Equal(date) {
				eventsComponents = append(eventsComponents, &cmpEvent{
					Header:    true,
					Timestamp: timestamp,
				})
			}
			lastDate = date

			if len(events) == 0 {
				cmp := &cmpEvent{
					ShowGroupHeader: true,
					Timestamp:       timestamp,
					TxId:            txId,
				}
				cmp.init(network)
				eventsComponents = append(eventsComponents, cmp)
			}

			for i, e := range events {
				eventComponent := &cmpEvent{
					ShowGroupHeader: i == 0,
					Timestamp:       timestamp,
					TxId:            txId,
					App:             e.App,
					Method:          e.Method,
				}
				eventComponent.init(network)
				eventsComponents = append(eventsComponents, eventComponent)

				switch {
				case e.Type < db.EventTypeSwap:
					transfer := e.Transfers[0]
					eventComponent.appendTransfers(transfer, walletsLabels, network, tokensBatch)
					eventComponent.Profit = &cmpEventProfit{
						Value:    transfer.Profit,
						Currency: "eur",
					}
				default:
					profitSum := decimal.Zero
					for _, t := range e.Transfers {
						eventComponent.appendTransfers(t, walletsLabels, network, tokensBatch)
						profitSum = profitSum.Add(t.Profit)
					}
					eventComponent.Profit = &cmpEventProfit{
						Value:    profitSum,
						Currency: "eur",
					}
				}
			}
		}
		if err := internalTxsRows.Err(); err != nil {
			return response{status: 500, err: err}
		}

		if err := tx.SendBatch(r.Context(), tokensBatchProcess(tokensBatch)).Close(); err != nil {
			return response{status: 500, err: err}
		}

		eventsPage := cmpEventsPage{
			Events: eventsComponents,
			UiPage: page,
		}
		if page > 0 {
			eventsPage.PrevUrl = fmt.Sprintf("/events?page=%d", page-1)
		}
		if !isLast {
			eventsPage.NextUrl = fmt.Sprintf("/events?page=%d", page+1)
		}

		eventsHtml = executeTemplateMust(templates, "events_page", eventsPage)
	}

	html := executeTemplateMust(templates, "dashboard_layout", cmpDashboard{
		Content:      template.HTML(eventsHtml),
		NavbarStatus: &navbarStatus,
	})
	return response{status: 200, data: html}
}
