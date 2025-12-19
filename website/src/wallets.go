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
	"unsafe"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
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

func defaultWalletLabel(address string) string {
	return fmt.Sprintf(
		"Wallet %s",
		address[len(address)-4:],
	)
}

type walletComponent struct {
	Label   string
	Address string
	TxCount int32

	NetworkImgUrl string
	AccountUrl    string
	Id            int32
	Status        string
	Insert        bool

	ShowFetch  bool
	ShowSseUrl bool

	LastSyncAt string
	ImportedAt string
}

func timeToRelativeStr(t time.Time) string {
	n := time.Now()
	if n.Day() != t.Day() {
		return t.Format("02/01/2006")
	}
	return t.Format("15:04:05")
}

func newWalletComponent(
	address string,
	txCount,
	id int32,
	network db.Network,
	status db.Status,
	lastSyncAt pgtype.Timestamp,
	importedAt time.Time,
) walletComponent {
	networkGlobals, ok := networksGlobals[network]
	assert.True(ok, "missing globals for %s network", network.String())

	s, ok := status.String()
	assert.True(ok, "invalid status %s", status)

	walletData := walletComponent{
		// TODO: real label
		Label:         fmt.Sprintf("Wallet %s", address[len(address)-4:]),
		Address:       address,
		TxCount:       txCount,
		Id:            id,
		Status:        s,
		NetworkImgUrl: networkGlobals.imgUrl,
		AccountUrl:    fmt.Sprintf("%s/%s", networkGlobals.explorerAccountUrl, address),
		ImportedAt:    timeToRelativeStr(importedAt),
	}

	if lastSyncAt.Valid {
		walletData.LastSyncAt = timeToRelativeStr(lastSyncAt.Time)
	} else {
		walletData.LastSyncAt = "-"
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
		var lastStatus db.Status

		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
			}

			const selectWalletStatus = `
				select
					status,
					address,
					network,
					tx_count,
					created_at,
					finished_at
				from
					wallet
				where
					user_account_id = $1 and
					id = $2 and
					delete_scheduled = false
			`
			row := pool.QueryRow(
				r.Context(), selectWalletStatus,
				userAccountId, walletId,
			)

			var status db.Status
			var address string
			var network db.Network
			var createdAt time.Time
			var finishedAt pgtype.Timestamp
			var txCount int32
			if err := row.Scan(&status, &address, &network, &txCount, &createdAt, &finishedAt); err != nil {
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
				txCount,
				walletId,
				network,
				status,
				finishedAt,
				createdAt,
			))
			sendSSEUpdate(w, flusher, string(html))

			lastStatus = status
		}
	}
}

type uiWalletStatus struct {
	Status db.Status
	Id     int32
}

// selectJobsQuery
//
//	select
//		type, status
//	from
//		worker_job
//	where
//		user_account_id = $1
//	order by
//		case
//			when type = 'parse_transactions' then 0
//			else 1
//		end
const selectJobsQuery string = `
	select
		type, status
	from
		worker_job
	where
		user_account_id = $1
	order by
		case
			when type = 'parse_transactions' then 0
			else 1
		end
`

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

	// TODO: this only returns if if atctually updates - NOT correct for deletion
	// resetJobsQuery
	//
	// 		update worker_job set
	// 			status = (
	// 				case
	// 					when status in ('in_progress', 'cancel_scheduled') then 'reset_scheduled'
	// 					else 'queued'
	// 				end
	// 			)::status
	// 		where
	// 			user_account_id = $1 and
	// 			status != 'queued'
	// 		returning
	// 			status, type
	const resetJobsQuery string = `
		update worker_job set
			status = (
				case
					when status in ('in_progress', 'cancel_scheduled') then 'reset_scheduled'
					else 'queued'
				end
			)::status
		where
			user_account_id = $1 and 
			status != 'queued'
		returning
			status, type
	`

	return func(w http.ResponseWriter, r *http.Request) {
		userAccountId := int32(1)
		// TODO: auth

		switch r.Method {
		case http.MethodGet:
			const selectWalletsQuery = `
				select
					id, label, address, network, status, tx_count, created_at, finished_at
				from
					wallet
				where
					user_account_id = $1 and
					delete_scheduled = false
				order by
					created_at desc
			`

			var responseStatus int
			var walletsRows []*walletComponent
			var jobs []*uiJob

			db.ExecuteTx(r.Context(), pool, func(ctx context.Context, tx pgx.Tx) error {
				batch := pgx.Batch{}

				batch.Queue(
					selectWalletsQuery, userAccountId,
				).Query(func(rows pgx.Rows) error {
					for rows.Next() {
						var walletId int32
						var walletLabel pgtype.Text
						var walletAddress string
						var network db.Network
						var status db.Status
						var txCount int32
						var finishedAt pgtype.Timestamp
						var createdAt time.Time
						if err := rows.Scan(
							&walletId, &walletLabel, &walletAddress, &network,
							&status, &txCount, &createdAt, &finishedAt,
						); err != nil {
							responseStatus = 500
							logger.Error("unable to scan wallets: %s", err)
							return err
						}

						if walletLabel.Valid == false {
							walletLabel.Valid = true
							walletLabel.String = defaultWalletLabel(walletAddress)
						}

						if status == db.StatusQueued || status == db.StatusInProgress {
							jobs = append(jobs, &uiJob{
								status: status,
								label:  walletLabel.String,
								t:      db.JobFetchWallet,
							})
						}

						walletData := newWalletComponent(
							walletAddress,
							txCount,
							walletId,
							network,
							status,
							finishedAt,
							createdAt,
						)
						walletsRows = append(walletsRows, &walletData)
					}

					return nil
				})

				batch.Queue(
					selectJobsQuery, userAccountId,
				).Query(func(rows pgx.Rows) error {
					for rows.Next() {
						var jobType db.JobType
						var jobStatus db.Status
						if err := rows.Scan(&jobType, &jobStatus); err != nil {
							responseStatus = 500
							logger.Error("unable to scan jobs: %s", err)
							return err
						}

						var label string
						switch jobType {
						case db.JobParseTransactions:
							label = "Parse transactions"
						case db.JobParseEvents:
							label = "Parse events"
						}

						jobs = append(jobs, &uiJob{
							status: jobStatus,
							label:  label,
							t:      jobType,
						})
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

			walletsPage := walletsComponent{
				WalletsCount: len(walletsRows),
				Wallets:      walletsRows,
			}
			walletsPageContent := executeTemplateMust(templates, "wallets_page", walletsPage)

			page := executeTemplateMust(templates, "dashboard_layout", dashboardPageData{
				Content:         template.HTML(walletsPageContent),
				StatusIndicator: newUiStatusIndicatorData(jobs),
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

			var responseStatus int
			var walletId, walletsCount int32
			var walletCreatedAt time.Time
			var jobs uiJobs

			err = db.ExecuteTx(
				r.Context(), pool,
				func(ctx context.Context, tx pgx.Tx) error {
					// insert wallet

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
						responseStatus = 409
						return errors.New("")
					}

					batch := pgx.Batch{}

					const insertWalletQuery = `
						insert into wallet (
							user_account_id, address, network, data, queued_at
						) values (
							$1, $2, $3, '{}'::jsonb, now()
						) returning
							id, created_at
					`
					batch.Queue(
						insertWalletQuery,
						userAccountId, walletAddress, network,
					).QueryRow(func(row pgx.Row) error {
						if err := row.Scan(&walletId, &walletCreatedAt); err != nil {
							logger.Error("unable to insert wallet: %s", err)
							return err
						}

						jobs = append(jobs, &uiJob{
							status: db.StatusQueued,
							label:  defaultWalletLabel(walletAddress),
							t:      db.JobFetchWallet,
						})
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

					// insert jobs

					const setJobsAsQueuedQuery = `
						insert into worker_job (
							user_account_id, type, status, queued_at
						) values
							($1, 'parse_transactions', 'queued', now()),
							($1, 'parse_events', 'queued', now())
						on conflict (user_account_id, type) do update set
							status = (
								case
									when worker_job.status in ('in_progress', 'cancel_scheduled') then 'reset_scheduled'
									else 'queued'
								end
							)::status,
							queued_at = now(),
							finished_at = null
						returning
							type, worker_job.status
					`
					rows, err := tx.Query(ctx, setJobsAsQueuedQuery, userAccountId)
					if err != nil {
						logger.Error("unable to insert jobs for wallet %d: %s", walletId, err)
						return err
					}
					for rows.Next() {
						if err := jobs.appendFromRows(rows); err != nil {
							responseStatus = 500
							logger.Error("unable to update jobs: %s", err.Error())
							return err
						}
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

			statusData := newUiStatusIndicatorData(jobs)
			if statusData.SseSubscribe {
				w.Header().Add("location", fmt.Sprintf(
					"/wallets/sse?id=%d,/jobs/sse",
					walletId,
				))
			} else {
				w.Header().Add("location", fmt.Sprintf("/wallets/sse?id=%d", walletId))
			}

			w.WriteHeader(202)

			walletData := newWalletComponent(
				walletAddress,
				0,
				walletId,
				network,
				db.StatusQueued,
				pgtype.Timestamp{},
				walletCreatedAt,
			)

			var html []byte

			if walletsCount == 1 {
				html = newResponseHtml(
					executeTemplateMust(templates, "wallets", walletsComponent{
						WalletsCount: 1,
						Wallets:      []*walletComponent{&walletData},
					}),
					executeTemplateMust(templates, "global_status", statusData),
				)
			} else {
				walletData.Insert = true
				html = newResponseHtml(
					executeTemplateMust(templates, "wallet", walletData),
					executeTemplateMust(templates, "wallets_count", walletsCount),
					executeTemplateMust(templates, "global_status", statusData),
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
			var jobs uiJobs

			db.ExecuteTx(r.Context(), pool, func(ctx context.Context, tx pgx.Tx) error {
				batch := pgx.Batch{}
				// delete wallet

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
						responseStatus = 404
						return errors.New("")
					}
					return nil
				})

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

				// TODO: rerun jobs but only if any wallets exist

				return nil
			})

			if responseStatus != 0 {
				w.WriteHeader(responseStatus)
				return
			}

			var html []byte
			if walletsCount == 0 {
				html = executeTemplateMust(templates, "wallets", walletsComponent{})
			} else {
				html = executeTemplateMust(templates, "wallets_count", walletsCount)
			}

			html = newResponseHtml(
				html,
				executeTemplateMust(templates, "global_status", newUiStatusIndicatorData(jobs)),
			)

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

			var newStatus db.Status
			if err := newStatus.ParseString(r.Form.Get("status")); err != nil {
				w.WriteHeader(400)
				w.Write(fmt.Appendf(nil, "status: %s", err.Error()))
				return
			}

			var jobs uiJobs

			switch newStatus {
			case db.StatusQueued:
				err := db.ExecuteTx(r.Context(), pool, func(ctx context.Context, tx pgx.Tx) error {
					// update wallet
					const queueWallet = `
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
							status in ('success', 'error', 'canceled')
						returning
							address, label
					`
					var address string
					var label pgtype.Text
					err := tx.QueryRow(
						ctx, queueWallet, userAccountId, walletId,
					).Scan(&address, &label)
					if errors.Is(err, pgx.ErrNoRows) {
						return fmt.Errorf("wallet not found: %w", pgx.ErrNoRows)
					}
					if err != nil {
						return fmt.Errorf("unable to update wallet: %w", err)
					}
					if !label.Valid {
						label.String = defaultWalletLabel(address)
					}
					jobs = append(jobs, &uiJob{
						status: db.StatusQueued,
						label:  label.String,
						t:      db.JobFetchWallet,
					})

					// reset jobs
					_, err = tx.Exec(ctx, resetJobsQuery, userAccountId)
					if err != nil {
						return fmt.Errorf("unable to reset jobs: %w", err)
					}

					rows, err := tx.Query(ctx, selectJobsQuery, userAccountId)
					if err != nil {
						return fmt.Errorf("unable to query jobs: %w", err)
					}
					for rows.Next() {
						if err := jobs.appendFromRows(rows); err != nil {
							return fmt.Errorf("unable to scan jobs: %w", err)
						}
					}
					if err := rows.Err(); err != nil {
						return fmt.Errorf("unable to reset jobs: %w", err)
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
					"/wallets/sse?id=%d,/jobs/sse",
					walletId,
				))
				w.WriteHeader(202)

				walletStatusData := uiWalletStatus{
					Status: db.StatusQueued,
					Id:     walletId,
				}
				walletStatusHtml := executeTemplateMust(
					templates,
					"wallet_status",
					&walletStatusData,
				)
				walletFetchButtnHtml := executeTemplateMust(
					templates,
					"wallet_fetch_button",
					&walletStatusData,
				)
				globalStatusHtml := executeTemplateMust(
					templates,
					"global_status",
					newUiStatusIndicatorData(jobs),
				)

				w.Write(newResponseHtml(
					walletStatusHtml,
					walletFetchButtnHtml,
					globalStatusHtml,
				))
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
						status
				`
				row := pool.QueryRow(
					r.Context(), setWalletAsCanceledQuery,
					userAccountId, walletId,
				)

				var status db.Status
				if err := row.Scan(&status); err != nil {
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

				walletStatusData := uiWalletStatus{
					Status: status,
					Id:     walletId,
				}
				walletStatusHtml := executeTemplateMust(
					templates,
					"wallet_status",
					&walletStatusData,
				)
				walletFetchButtnHtml := executeTemplateMust(
					templates,
					"wallet_fetch_button",
					&walletStatusData,
				)

				w.Write(newResponseHtml(walletStatusHtml, walletFetchButtnHtml))
			default:
				w.WriteHeader(400)
				w.Write([]byte("status: invalid"))
			}
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}
