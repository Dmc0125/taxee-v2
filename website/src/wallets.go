package main

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"taxee/pkg/assert"
	"taxee/pkg/db"
	"taxee/pkg/logger"
	"time"
	"unsafe"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mr-tron/base58"
)

type paramGetter interface {
	Get(string) string
}

func parseIntParam[T any](
	getter paramGetter,
	result *T,
	key string,
	positiveOnly bool,
	size int,
) error {
	value := getter.Get(key)

	if value == "" {
		return fmt.Errorf("%s: missing", key)
	}

	if v, err := strconv.ParseInt(value, 10, size); err == nil {
		if positiveOnly && v < 0 {
			return fmt.Errorf("%s: must be positive", key)
		}
		*result = *(*T)(unsafe.Pointer(&v))
		return nil
	} else {
		return fmt.Errorf("%s: %w", key, err)
	}
}

func parseUintParam[T any](
	getter paramGetter,
	result *T,
	key string,
	size int,
) error {
	value := getter.Get(key)

	if value == "" {
		return fmt.Errorf("%s: missing", key)
	}

	if v, err := strconv.ParseUint(value, 10, size); err == nil {
		*result = *(*T)(unsafe.Pointer(&v))
		return nil
	} else {
		return fmt.Errorf("%s: %w", key, err)
	}
}

type walletComponent struct {
	Address       string
	NetworkImgUrl string
	Id            int32
	Status        uint8
	Insert        bool

	ShowFetch  bool
	ShowSseUrl bool
}

func newWalletComponent(
	address string,
	id int32,
	network db.Network,
	status db.Status,
) walletComponent {
	networkGlobals, ok := networksGlobals[network]
	assert.True(ok, "missing globals for %s network", network.String())

	walletData := walletComponent{
		Address:       address,
		Id:            id,
		Status:        uint8(status),
		NetworkImgUrl: networkGlobals.imgUrl,
	}

	switch status {
	case db.StatusSuccess, db.StatusError, db.StatusCanceled:
		walletData.ShowFetch = true
	case db.StatusQueued, db.StatusInProgress:
		walletData.ShowSseUrl = true
	}

	return walletData
}

type walletsComponent struct {
	WalletsCount int
	Wallets      []*walletComponent
}

func newResponseHtml(fragments ...[]byte) []byte {
	var sum []byte
	for i, f := range fragments {
		sum = append(sum, f...)
		if i < len(fragments)-1 {
			sum = append(sum, []byte("<!--delimiter-->")...)
		}
	}
	return sum
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
		lastStatus := db.Status(255)

		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
			}

			const selectWalletStatus = `
				select status, address, network from wallet where
					user_account_id = $1 and
					id = $2 and
					delete_scheduled = false
			`
			var status db.Status
			var address string
			var network db.Network
			row := pool.QueryRow(
				r.Context(), selectWalletStatus,
				userAccountId, walletId,
			)
			if err := row.Scan(&status, &address, &network); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					sendSSEClose(w, flusher)
					return
				}
				logger.Error("unable to select status: %s", err.Error())
				sendSSEError(w, flusher, err.Error())
				return
			}

			if status == lastStatus {
				continue
			}

			html := executeTemplateMust(templates, "wallet", newWalletComponent(
				address,
				walletId,
				network,
				status,
			))
			sendSSEUpdate(w, flusher, string(html))

			lastStatus = status
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
			w.delete_scheduled = false
	`

	return func(w http.ResponseWriter, r *http.Request) {
		userAccountId := int32(1)
		// TODO: auth

		switch r.Method {
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

			switch network {
			case db.NetworkSolana:
				walletBytes, err := base58.Decode(walletAddress)
				if err != nil {
					w.WriteHeader(400)
					w.Write([]byte("walletAddress is not valid base58"))
					return
				}
				if len(walletBytes) != 32 {
					w.WriteHeader(400)
					w.Write([]byte("walletAddress invalid len"))
					return
				}
			default:
				w.WriteHeader(400)
				w.Write([]byte("network invalid"))
				return
			}

			/////////////
			// insert
			var status int
			var walletId, walletsCount int32

			err = db.ExecuteTx(
				r.Context(), pool,
				func(ctx context.Context, tx pgx.Tx) error {
					const selectWalletQuery = `
						select true from wallet w where
							w.address = $1 and
							w.network = $2 and
							w.delete_scheduled = false
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
						status = 409
						return errors.New("")
					}

					batch := pgx.Batch{}

					const insertWalletQuery = `
						insert into wallet (
							user_account_id, address, network, data, queued_at
						) values (
							$1, $2, $3, '{}'::jsonb, now()
						) returning
							id
					`
					batch.Queue(
						insertWalletQuery,
						userAccountId, walletAddress, network,
					).QueryRow(func(row pgx.Row) error {
						if err := row.Scan(&walletId); err != nil {
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

					return tx.SendBatch(ctx, &batch).Close()
				},
			)

			if err != nil {
				if status == 0 {
					status = 500
				}
				w.WriteHeader(status)
				return
			}

			w.Header().Add("location", fmt.Sprintf("/wallets/sse?id=%d", walletId))
			w.WriteHeader(202)

			walletData := newWalletComponent(
				walletAddress,
				walletId,
				network,
				db.StatusQueued,
			)
			var html []byte

			if walletsCount == 1 {
				html = executeTemplateMust(templates, "wallets", walletsComponent{
					WalletsCount: 1,
					Wallets:      []*walletComponent{&walletData},
				})
			} else {
				walletData.Insert = true
				html = newResponseHtml(
					executeTemplateMust(templates, "wallet", walletData),
					executeTemplateMust(templates, "wallets_count", walletsCount),
				)
			}

			w.Write(html)
		case http.MethodGet:
			const selectWalletsQuery = `
				select 
					id, address, network, status
				from 
					wallet
				where
					user_account_id = $1 and 
					delete_scheduled = false
				order by
					network asc,
					created_at desc
			`
			rows, err := pool.Query(r.Context(), selectWalletsQuery, userAccountId)
			if err != nil {
				w.WriteHeader(500)
				logger.Info("unable to query wallets: %s", err)
				return
			}

			var walletsRows []*walletComponent

			for rows.Next() {
				var walletId int32
				var walletAddress string
				var network db.Network
				var status db.Status
				if err := rows.Scan(
					&walletId, &walletAddress, &network, &status,
				); err != nil {
					w.WriteHeader(500)
					w.Write(fmt.Appendf(nil, "unable to scan wallets: %s", err))
					return
				}

				walletData := newWalletComponent(
					walletAddress,
					walletId,
					network,
					status,
				)
				walletsRows = append(walletsRows, &walletData)
			}

			if err := rows.Err(); err != nil {
				w.WriteHeader(500)
				w.Write(fmt.Appendf(nil, "unable to read wallets: %s", err))
				return
			}

			walletsPage := walletsComponent{
				WalletsCount: len(walletsRows),
				Wallets:      walletsRows,
			}

			walletsPageContent := executeTemplateMust(templates, "wallets_page", walletsPage)

			page := executeTemplateMust(templates, "dashboard_layout", pageLayoutComponentData{
				Content: template.HTML(walletsPageContent),
			})
			w.WriteHeader(200)
			w.Write(page)
		case http.MethodDelete:
			var walletId int32
			if err := parseIntParam(r.URL.Query(), &walletId, "id", true, 32); err != nil {
				w.WriteHeader(400)
				w.Write([]byte(err.Error()))
				return
			}

			var status int
			var walletsCount int32
			err := db.ExecuteTx(r.Context(), pool, func(ctx context.Context, tx pgx.Tx) error {
				batch := pgx.Batch{}
				const scheduleWalletForDeletionQuery = `
					update wallet set
						delete_scheduled = true,
						status = (
							case
								when status = 'in_progress' then 'cancel_scheduled'::status
								else status
							end
						)
					where
						user_account_id = $1 and id = $2 and
						delete_scheduled = false
				`
				batch.Queue(
					scheduleWalletForDeletionQuery, userAccountId, walletId,
				).Exec(func(ct pgconn.CommandTag) error {
					if ct.RowsAffected() == 0 {
						status = 404
						return errors.New("")
					}
					return nil
				})

				batch.Queue(selectWalletsCountQuery, userAccountId).QueryRow(func(row pgx.Row) error {
					if err := row.Scan(&walletsCount); err != nil {
						logger.Error("unable to select wallets count: %s", err)
						return err
					}
					return nil
				})

				return tx.SendBatch(ctx, &batch).Close()
			})

			if err != nil {
				if status == 0 {
					status = 500
				}
				w.WriteHeader(status)
				return
			}

			var html []byte
			if walletsCount == 0 {
				html = executeTemplateMust(templates, "wallets", walletsComponent{})
			} else {
				html = executeTemplateMust(templates, "wallets_count", walletsCount)
			}

			w.WriteHeader(200)
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

			var newStatus uint8
			if err := parseUintParam(r.Form, &newStatus, "status", 8); err != nil {
				w.WriteHeader(400)
				w.Write([]byte(err.Error()))
				return
			}

			switch db.Status(newStatus) {
			case db.StatusQueued:
				const setWalletAsQueuedQuery = `
					update wallet set
						status = 'queued',
						queued_at = now(),
						fresh = (
							case
								when status = 'error' or status = 'canceled' then true
								else false
							end
						)
					where
						user_account_id = $1 and
						id = $2 and
						delete_scheduled = false and
						(
							status = 'success' or
							status = 'error' or
							status = 'canceled'
						)
					returning
						address, network
				`
				row := pool.QueryRow(
					r.Context(), setWalletAsQueuedQuery,
					userAccountId, walletId,
				)

				var address string
				var network db.Network
				if err := row.Scan(&address, &network); err != nil {
					if errors.Is(err, pgx.ErrNoRows) {
						w.WriteHeader(404)
						return
					}
					w.WriteHeader(500)
					logger.Error("unable to update wallet: %s", err.Error())
					return
				}

				w.Header().Add("location", fmt.Sprintf("/wallets/sse?id=%d", walletId))
				w.WriteHeader(202)

				html := executeTemplateMust(templates, "wallet", newWalletComponent(
					address,
					walletId,
					network,
					db.StatusQueued,
				))
				w.Write(html)
			case db.StatusCanceled:
				const setWalletAsCanceledQuery = `
					update wallet set
						status = (
							case
								when status = 'queued' then 'canceled'
								when status = 'in_progress' then 'cancel_scheduled'
							end
						)::status
					where
						user_account_id = $1 and
						id = $2 and
						delete_scheduled = false and
						(status = 'queued' or status = 'in_progress')
					returning
						status, address, network
				`
				row := pool.QueryRow(
					r.Context(), setWalletAsCanceledQuery,
					userAccountId, walletId,
				)

				var status db.Status
				var address string
				var network db.Network
				if err := row.Scan(&status, &address, &network); err != nil {
					if errors.Is(err, pgx.ErrNoRows) {
						w.WriteHeader(404)
						return
					}
					w.WriteHeader(500)
					logger.Error("unable to update wallet: %s", err.Error())
					return
				}

				if status == db.StatusCancelScheduled {
					w.Header().Add("location", fmt.Sprintf("/wallets/sse?id=%d", walletId))
					w.WriteHeader(202)
				} else {
					w.WriteHeader(200)
				}

				html := executeTemplateMust(templates, "wallet", newWalletComponent(
					address,
					walletId,
					network,
					status,
				))
				w.Write(html)
			}
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}
