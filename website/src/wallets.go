package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"taxee/pkg/assert"
	"taxee/pkg/db"
	"taxee/pkg/logger"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mr-tron/base58"
)

// components

type cmpWalletStatus struct {
	Id     int32
	Status db.WalletStatus
}

type cmpWalletFetchButton struct {
	Id       int32
	Disabled bool
}

type cmpWallet struct {
	Id            int32
	Label         string
	Address       string
	TxCount       int32
	LastSyncAt    pgtype.Timestamp
	NetworkImgUrl string
	AccountUrl    string

	Status      db.WalletStatus
	FetchButton *cmpWalletFetchButton

	Insert       bool
	SseSubscribe bool
}

// Expects that address, id and status have been set
func (w *cmpWallet) init(network db.Network) {
	// network stuff
	n, ok := networksGlobals[network]
	assert.True(ok, "missing globals for network: %d", network)

	w.NetworkImgUrl = n.imgUrl
	assert.True(len(w.Address) > 0, "wallet address is not set")
	w.AccountUrl = fmt.Sprintf("%s/%s", n.explorerAccountUrl, w.Address)

	// fetch button
	w.FetchButton = &cmpWalletFetchButton{
		Id: w.Id,
	}

	switch w.Status {
	case db.WalletQueued, db.WalletInProgress:
		w.FetchButton.Disabled = true
		w.SseSubscribe = true
	}
}

type cmpWallets struct {
	WalletsCount int
	Wallets      []*cmpWallet
}

func walletsSseHandler(pool *pgxpool.Pool, templates *template.Template) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		userAccountId := int32(1)
		// TODO: auth

		flusher, ok := initSSEHandler(w)
		if !ok {
			return
		}

		var walletId int32
		if err := parseIntParam(r.URL.Query(), &walletId, "id", true, 32); err != nil {
			sendSSEError(w, flusher, err.Error())
			return
		}

		ticker := time.NewTicker(time.Second)
		var lastStatus db.WalletStatus

		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
			}

			const selectWallet = `
				select
					id, status, address, network,
					label, tx_count, finished_at
				from
					wallet
				where
					user_account_id = $1 and
					id = $2 and
					status != 'delete'
			`
			row := pool.QueryRow(
				r.Context(), selectWallet,
				userAccountId, walletId,
			)

			var wallet cmpWallet
			var network db.Network
			if err := row.Scan(
				&wallet.Id, &wallet.Status, &wallet.Address, &network,
				&wallet.Label, &wallet.TxCount, &wallet.LastSyncAt,
			); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					sendSSEClose(w, flusher)
					return
				}
				logger.Error("unable to select wallet: %s", err.Error())
				sendSSEError(w, flusher, err.Error())
				return
			}

			if wallet.Status == lastStatus {
				continue
			}

			wallet.init(network)
			html := executeTemplateMust(templates, "wallet", wallet)
			sendSSEUpdate(w, flusher, string(html))

			lastStatus = wallet.Status
		}
	}
}

func walletsHandler(
	pool *pgxpool.Pool,
	templates *template.Template,
) http.HandlerFunc {
	// selectWalletsCountQuery
	//
	// 		select count(w.id) from wallet w where
	// 			w.user_account_id = $1 and
	// 			w.delete_scheduled = false
	const selectWalletsCountQuery string = `
		select count(w.id) from wallet w where
			w.user_account_id = $1 and
			status != 'delete'
	`

	const selectParserQuery string = `
		select status from parser where user_account_id = $1
	`

	const resetParser = `
		update parser set
			status = (
				case
					when status in ('pt_in_progress', 'pe_in_progress') then 'reset'
					else 'pt_queued'
				end
			)::parser_status,
			queued_at = (
				case 
					when status = 'pe_queued' then queued_at
					else now()
				end
			)
		where
			status != 'pt_queued' and
			user_account_id = $1
	`

	return func(w http.ResponseWriter, r *http.Request) {
		userAccountId := int32(1)
		// TODO: auth

		switch r.Method {
		case http.MethodGet:
			const selectWalletsQuery = `
				select
					id, label, address, network, status, tx_count, finished_at
				from
					wallet
				where
					user_account_id = $1 and
					status != 'delete'
				order by
					created_at desc
			`

			var responseStatus int
			var walletsRows []*cmpWallet
			var navbarStatus cmpNavbarStatus

			db.ExecuteTx(r.Context(), pool, func(ctx context.Context, tx pgx.Tx) error {
				batch := pgx.Batch{}

				batch.Queue(
					selectWalletsQuery, userAccountId,
				).Query(func(rows pgx.Rows) error {
					for rows.Next() {
						var wallet cmpWallet
						var network db.Network
						if err := rows.Scan(
							&wallet.Id, &wallet.Label, &wallet.Address, &network,
							&wallet.Status, &wallet.TxCount, &wallet.LastSyncAt,
						); err != nil {
							responseStatus = 500
							logger.Error("unable to scan wallets: %s", err)
							return err
						}

						navbarStatus.appendWallet(wallet.Label, wallet.Status)

						wallet.init(network)
						walletsRows = append(walletsRows, &wallet)
					}

					return nil
				})

				batch.Queue(selectParserQuery, userAccountId).QueryRow(func(row pgx.Row) error {
					if err := navbarStatus.appendParserFromRow(row); err != nil {
						logger.Error("unable to query parser: %s", err)
						return err
					}
					return nil
				})

				if err := tx.SendBatch(ctx, &batch).Close(); err != nil {
					responseStatus = 500
					logger.Error("unable to execute batch: %s", err)
					return err
				}

				return nil
			})

			if responseStatus != 0 {
				w.WriteHeader(responseStatus)
				return
			}

			if navbarStatus.Status == navbarStatusInProgress {
				navbarStatus.SseSubscribe = true
			}

			walletsPage := cmpWallets{
				WalletsCount: len(walletsRows),
				Wallets:      walletsRows,
			}
			walletsPageContent := executeTemplateMust(templates, "wallets_page", walletsPage)

			page := executeTemplateMust(templates, "dashboard_layout", cmpDashboard{
				Content:      template.HTML(walletsPageContent),
				NavbarStatus: &navbarStatus,
			})
			w.WriteHeader(200)
			w.Write(page)
		case http.MethodPost:
			err := r.ParseMultipartForm(10 << 20)
			if err != nil {
				w.WriteHeader(500)
				w.Write(fmt.Appendf(nil, "unable to read request body: %s", err))
				return
			}

			var network db.Network
			if v, err := strconv.ParseUint(r.FormValue("network"), 10, 16); err == nil {
				network = db.Network(v)
			} else {
				w.WriteHeader(400)
				w.Write(fmt.Appendf(nil, "invalid network: %s", err))
				return
			}

			walletAddress := r.FormValue("wallet_address")

			switch {
			case network == db.NetworkSolana:
				walletBytes, err := base58.Decode(walletAddress)
				if err != nil {
					w.WriteHeader(400)
					w.Write([]byte("walletAddress: not valid base58"))
					return
				}
				if len(walletBytes) != 32 {
					w.WriteHeader(400)
					w.Write([]byte("walletAddress: invalid len"))
					return
				}
			case network > db.NetworkEvmStart && network < db.NetworksCount:
				if len(walletAddress) != 42 {
					w.WriteHeader(400)
					w.Write([]byte("walletAddress: invalid len"))
					return
				}
				if !strings.HasPrefix(walletAddress, "0x") {
					w.WriteHeader(400)
					w.Write([]byte("walletAddress: invalid prefix"))
					return
				}
				if _, err := hex.DecodeString(walletAddress[2:]); err != nil {
					w.WriteHeader(400)
					w.Write([]byte("walletAddress: not valid hex"))
					return
				}
			default:
				w.WriteHeader(400)
				w.Write([]byte("network invalid"))
				return
			}

			walletLabel := r.Form.Get("label")
			if walletLabel == "" {
				walletLabel = fmt.Sprintf("Wallet %s", walletAddress[len(walletAddress)-4:])
			}

			var responseStatus int
			var walletsCount int32
			wallet := cmpWallet{
				Address: walletAddress,
				Label:   walletLabel,
				Status:  db.WalletQueued,
			}

			err = db.ExecuteTx(
				r.Context(), pool,
				func(ctx context.Context, tx pgx.Tx) error {
					// insert wallet

					const selectWalletQuery = `
						select true from wallet w where
							w.address = $1 and
							w.network = $2 and
							status != 'delete'
					`
					var walletFound bool
					row := tx.QueryRow(
						ctx, selectWalletQuery,
						walletAddress, network,
					)
					if err := row.Scan(&walletFound); err != nil && !errors.Is(err, pgx.ErrNoRows) {
						logger.Error("unable to select wallet: %s", err.Error())
						return err
					}
					if walletFound {
						responseStatus = 409
						return errors.New("")
					}

					batch := pgx.Batch{}

					const insertWalletQuery = `
						insert into wallet (
							user_account_id, address, network, data, queued_at, label
						) values (
							$1, $2, $3, '{}'::jsonb, now(), $4
						) returning
							id
					`
					batch.Queue(
						insertWalletQuery,
						userAccountId, walletAddress, network, walletLabel,
					).QueryRow(func(row pgx.Row) error {
						if err := row.Scan(&wallet.Id); err != nil {
							logger.Error("unable to insert wallet: %s", err)
							return err
						}

						return nil
					})

					batch.Queue(
						selectWalletsCountQuery, userAccountId,
					).QueryRow(func(row pgx.Row) error {
						if err := row.Scan(&walletsCount); err != nil {
							logger.Error("unable to select wallets count: %s", err)
							return err
						}
						return nil
					})

					if err := tx.SendBatch(ctx, &batch).Close(); err != nil {
						return err
					}

					// TODO: insert or reset parser
					const insertOrResetParser = `
						insert into parser (
							user_account_id, status, queued_at
						) values (
							$1, 'pt_queued', now()
						) on conflict (user_account_id) do update set
							status = 'pt_queued',
							queued_at = now()
						where
							parser.status != 'pt_queued'
					`
					if _, err := tx.Exec(ctx, insertOrResetParser, userAccountId); err != nil {
						logger.Error("unable to insert parser: %s", err)
						return err
					}

					return nil
				},
			)

			if err != nil {
				if responseStatus == 0 {
					responseStatus = 500
				}
				w.WriteHeader(responseStatus)
				return
			}

			wallet.init(network)

			w.Header().Add(
				"location",
				fmt.Sprintf("/wallets/sse?id=%d,/parser/sse", wallet.Id),
			)
			w.WriteHeader(202)

			var html []byte

			if walletsCount == 1 {
				html = newResponseHtml(
					executeTemplateMust(templates, "wallets", cmpWallets{
						WalletsCount: 1,
						Wallets:      []*cmpWallet{&wallet},
					}),
				)
			} else {
				wallet.Insert = true
				html = newResponseHtml(
					executeTemplateMust(templates, "wallet", wallet),
					executeTemplateMust(templates, "wallets_count", walletsCount),
				)
			}

			w.Write(html)
		case http.MethodDelete:
			var walletId int32
			if err := parseIntParam(r.URL.Query(), &walletId, "id", true, 32); err != nil {
				w.WriteHeader(400)
				w.Write([]byte(err.Error()))
				return
			}

			var responseStatus int
			var walletsCount int32

			db.ExecuteTx(r.Context(), pool, func(ctx context.Context, tx pgx.Tx) error {
				batch := pgx.Batch{}
				// delete wallet

				const deleteWallet = `
					delete from wallet where id = $1 and status != 'in_progress'
				`
				tag, err := tx.Exec(ctx, deleteWallet, walletId)
				if err != nil {
					logger.Error("unable to delete wallet: %s", err.Error())
					return err
				}
				if tag.RowsAffected() == 0 {
					const updateWallet = `
						update wallet set
							status = 'delete'
						where
							user_account_id = $1 and id = $2
					`
					batch.Queue(
						updateWallet, userAccountId, walletId,
					).Exec(func(ct pgconn.CommandTag) error {
						if ct.RowsAffected() == 0 {
							responseStatus = 404
							return errors.New("")
						}
						return nil
					})
				}

				batch.Queue(selectWalletsCountQuery, userAccountId).QueryRow(func(row pgx.Row) error {
					if err := row.Scan(&walletsCount); err != nil {
						responseStatus = 500
						logger.Error("unable to select wallets count: %s", err)
						return err
					}
					return nil
				})

				if err := tx.SendBatch(ctx, &batch).Close(); err != nil {
					responseStatus = 500
					logger.Error("unable to execute batch: %s", err)
					return err
				}

				// reset parser

				if _, err := tx.Exec(ctx, resetParser, userAccountId); err != nil {
					responseStatus = 500
					logger.Error("unable to reset parser: %s", err.Error())
					return err
				}

				return nil
			})

			if responseStatus != 0 {
				w.WriteHeader(responseStatus)
				return
			}

			var html []byte
			if walletsCount == 0 {
				html = executeTemplateMust(templates, "wallets", cmpWallet{})
			} else {
				html = executeTemplateMust(templates, "wallets_count", walletsCount)
			}

			w.Header().Add("location", "/parser/sse")
			w.WriteHeader(202)
			w.Write(html)
		case http.MethodPut:
			err := r.ParseMultipartForm(10 << 20)
			if err != nil {
				w.WriteHeader(500)
				logger.Error("unable to parse form data: %s", err)
				return
			}

			var walletId int32
			if err := parseIntParam(r.Form, &walletId, "id", true, 32); err != nil {
				w.WriteHeader(400)
				w.Write([]byte(err.Error()))
				return
			}

			var newStatus db.WalletStatus
			if err := newStatus.ParseString(r.Form.Get("status")); err != nil {
				w.WriteHeader(400)
				w.Write(fmt.Appendf(nil, "status: %s", err.Error()))
				return
			}

			switch newStatus {
			case db.WalletQueued:
				err := db.ExecuteTx(r.Context(), pool, func(ctx context.Context, tx pgx.Tx) error {
					// update wallet
					const queueWallet = `
						update wallet set
							status = 'queued',
							queued_at = now(),
							fresh = (
								case
									when status = 'error' then true
									else false
								end
							)
						where
							user_account_id = $1 and
							id = $2 and
							status in ('success', 'error')
					`
					tag, err := tx.Exec(
						ctx, queueWallet, userAccountId, walletId,
					)
					if tag.RowsAffected() == 0 {
						return fmt.Errorf("wallet not found: %w", pgx.ErrNoRows)
					}
					if err != nil {
						return fmt.Errorf("unable to update wallet: %w", err)
					}

					// reset parser
					if _, err := tx.Exec(ctx, resetParser, userAccountId); err != nil {
						return fmt.Errorf("unable to reset parser: %w", err)
					}

					return nil
				})

				if err != nil {
					if errors.Is(err, pgx.ErrNoRows) {
						w.WriteHeader(404)
					} else {
						w.WriteHeader(500)
					}
					logger.Error(err.Error())
					return
				}

				w.Header().Add("location", fmt.Sprintf(
					"/wallets/sse?id=%d,/parser/sse",
					walletId,
				))
				w.WriteHeader(202)

				cmpStatus := cmpWalletStatus{
					Status: db.WalletQueued,
					Id:     walletId,
				}
				walletStatusHtml := executeTemplateMust(
					templates,
					"wallet_status",
					&cmpStatus,
				)

				fetchButton := cmpWalletFetchButton{
					Id:       walletId,
					Disabled: true,
				}
				walletFetchButtonHtml := executeTemplateMust(
					templates,
					"wallet_fetch_button",
					&fetchButton,
				)

				w.Write(newResponseHtml(
					walletStatusHtml,
					walletFetchButtonHtml,
				))
			default:
				w.WriteHeader(400)
				w.Write([]byte("status: invalid"))
			}
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}
