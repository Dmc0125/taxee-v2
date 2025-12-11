package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"taxee/pkg/assert"
	"taxee/pkg/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mr-tron/base58"
)

// isertWorkerJobQuery
//
//	insert into worker_job (
//		id, type, data, user_account_id
//	) values (
//		$1, $2, $3, $4
//	) on conflict (
//		user_account_id, type, data
//	) do nothing
const insertWorkerJobQuery string = `
	insert into worker_job (
		id, type, data, user_account_id
	) values (
		$1, $2, $3, $4
	) on conflict (
		user_account_id, type, data
	) do nothing
`

type walletComponent struct {
	Address       string
	NetworkImgUrl string
	Id            int32
	Status        uint8
	ShowFetch     bool
	Insert        bool
	JobId         string
	SseUrl        string
}

func newWalletComponent(
	address string,
	id int32,
	network db.Network,
	status uint8,
	jobId uuid.UUID,
) walletComponent {
	networkGlobals, ok := networksGlobals[network]
	assert.True(ok, "missing globals for %s network", network.String())

	walletData := walletComponent{
		Address:       address,
		Id:            id,
		Status:        status,
		NetworkImgUrl: networkGlobals.imgUrl,
		JobId:         jobId.String(),
	}

	switch status {
	case db.WorkerJobSuccess, db.WorkerJobError, db.WorkerJobCanceled:
		walletData.ShowFetch = true
	case db.WorkerJobQueued, db.WorkerJobInProgress:
		walletData.SseUrl = fmt.Sprintf("/jobs?id=%s", jobId.String())

		// TODO: schduled cancel
	}

	return walletData
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

			tx, err := pool.Begin(context.Background())
			if err != nil {
				w.WriteHeader(500)
				w.Write([]byte("unable to begin tx"))
				return
			}
			defer tx.Rollback(r.Context())

			// check if wallet exists
			const selectWalletQuery = `
				select true from wallet w where
					w.address = $1 and
					w.network = $2 and
					w.delete_scheduled = false
			`
			var walletFound bool
			row := tx.QueryRow(
				r.Context(), selectWalletQuery,
				walletAddress, network,
			)
			if err := row.Scan(&walletFound); err != nil {
				if !errors.Is(err, pgx.ErrNoRows) {
					w.WriteHeader(500)
					w.Write(fmt.Appendf(nil, "unable to query wallet: %s", err))
					return
				}
			}

			if walletFound {
				w.WriteHeader(409)
				return
			}

			batch := pgx.Batch{}
			var responseStatus int
			var responseData []byte

			const insertWalletQuery = `
				insert into wallet (
					user_account_id, address, network, name, data
				) values (
					$1, $2, $3::network, $4, '{}'::jsonb
				) returning 
					id
			`
			var walletId int32
			batch.Queue(
				insertWalletQuery,
				userAccountId, walletAddress, network, pgtype.Text{},
			).QueryRow(func(row pgx.Row) error {
				if err := row.Scan(&walletId); err != nil {
					responseStatus = 500
					responseData = fmt.Appendf(nil, "unable to insert wallet: %s", err)
					return err
				}
				return nil
			})

			const updateStatsQuery = `
				insert into stats (user_account_id) values ($1) on conflict (
					user_account_id
				) do nothing
			`
			batch.Queue(updateStatsQuery, userAccountId)

			var walletsCount int32
			batch.Queue(selectWalletsCountQuery, userAccountId).QueryRow(func(row pgx.Row) error {
				if err := row.Scan(&walletsCount); err != nil {
					responseStatus = 500
					responseData = fmt.Appendf(nil, "unable to update stats: %s", err)
					return err
				}
				return nil
			})

			br := tx.SendBatch(context.Background(), &batch)
			if err := br.Close(); err != nil {
				if responseStatus != 0 {
					w.WriteHeader(responseStatus)
					w.Write(responseData)
				} else {
					w.WriteHeader(500)
					w.Write(fmt.Appendf(nil, "unable to execute batch: %s", err))
				}
				return
			}

			workerData, err := json.Marshal(db.WorkerJobFetchWalletData{
				WalletId:      walletId,
				WalletAddress: walletAddress,
				Network:       network,
			})
			assert.NoErr(err, "unable to serialize worker message data")

			jobId, err := uuid.NewRandom()
			if err != nil {
				w.WriteHeader(500)
				w.Write(fmt.Appendf(nil, "unable to create uuid: %s", err.Error()))
				return
			}

			_, err = tx.Exec(
				context.Background(), insertWorkerJobQuery,
				jobId, db.WorkerJobFetchWallet, workerData, userAccountId,
			)
			if err != nil {
				w.WriteHeader(500)
				w.Write(fmt.Appendf(nil, "unable to insert parser message: %s", err))
				return
			}

			if err := tx.Commit(context.Background()); err != nil {
				w.WriteHeader(500)
				w.Write(fmt.Appendf(nil, "unable to execute transaction: %s", err))
				return
			}

			walletData := newWalletComponent(
				walletAddress,
				walletId,
				network,
				db.WorkerJobQueued,
				jobId,
			)

			var html []byte
			if walletsCount == 1 {
				html = executeTemplateMust(templates, "wallets_table", []walletComponent{walletData})
			} else {
				walletData.Insert = true
				html = executeTemplateMust(templates, "wallet", walletData)
			}

			w.Header().Add("location", fmt.Sprintf("/jobs?id=%s", jobId.String()))
			w.WriteHeader(202)
			w.Write(html)
		case http.MethodGet:
			// select all wallets which are not scheduled to be deleted (message
			// type != 2) and also select their corresponding fetch job or message
			const selectWalletsQuery = `
				select 
					w.id, 
					w.address, 
					w.network,
					wj.id,
					wj.status
				from 
					wallet w
				join
					worker_job wj on
						wj.type = 0 and
						(wj.data->>'walletId')::integer = w.id
				where
					w.user_account_id = $1 and 
					w.delete_scheduled = false
				order by
					w.created_at desc
			`
			rows, err := pool.Query(context.Background(), selectWalletsQuery, userAccountId)
			if err != nil {
				w.WriteHeader(500)
				w.Write(fmt.Appendf(nil, "unable to query wallets: %s", err))
				return
			}

			var walletsRows []*walletComponent

			for rows.Next() {
				var walletId int32
				var walletAddress string
				var network db.Network
				var jobId uuid.UUID
				var jobStatus uint8
				if err := rows.Scan(
					&walletId, &walletAddress, &network,
					&jobId, &jobStatus,
				); err != nil {
					w.WriteHeader(500)
					w.Write(fmt.Appendf(nil, "unable to scan wallets: %s", err))
					return
				}

				walletData := newWalletComponent(
					walletAddress,
					walletId,
					network,
					jobStatus,
					jobId,
				)
				walletsRows = append(walletsRows, &walletData)
			}

			if err := rows.Err(); err != nil {
				w.WriteHeader(500)
				w.Write(fmt.Appendf(nil, "unable to read wallets: %s", err))
				return
			}

			walletsPageContent := executeTemplateMust(templates, "wallets_page", walletsRows)

			page := executeTemplateMust(templates, "dashboard_layout", pageLayoutComponentData{
				Content: template.HTML(walletsPageContent),
			})
			w.WriteHeader(200)
			w.Write(page)
		case http.MethodDelete:
			// check if job is in progress
			// if it is -> insert cancel message
			// if job does not exist -> delete fetch wallet messages
			// insert delete wallet message

			var walletId int32
			if ok := parseIntParam(w, paramGetter(r.URL.Query()), &walletId, "id", true, 32); !ok {
				return
			}

			tx, err := pool.Begin(r.Context())
			if err != nil {
				w.WriteHeader(500)
				w.Write([]byte("unable to begin tx"))
				return
			}
			defer tx.Rollback(r.Context())

			batch := pgx.Batch{}

			const scheduleWalletForDeletionQuery = `
				update wallet set
					delete_scheduled = true
				where
					user_account_id = $1 and
					id = $2 and
					delete_scheduled = false
			`
			batch.Queue(scheduleWalletForDeletionQuery, userAccountId, walletId)
			var walletsCount int32
			batch.Queue(selectWalletsCountQuery, userAccountId).QueryRow(func(row pgx.Row) error {
				return row.Scan(&walletsCount)
			})

			// TODO: maybe return wallet with canceled state if this sets anything ?
			const setJobScheduledForCancelQuery = `
				update worker_job set
					status = $1::smallint
				where
					type = 0 and 
					(data->>'walletId')::integer = $2 and
					status = $3::smallint
			`
			batch.Queue(
				setJobScheduledForCancelQuery,
				db.WorkerJobCancelScheduled, walletId, db.WorkerJobInProgress,
			)

			br := tx.SendBatch(r.Context(), &batch)
			if err := br.Close(); err != nil {
				w.WriteHeader(500)
				w.Write(fmt.Appendf(nil, "unable to delete wallet: %s", err))
				return
			}

			if err := tx.Commit(context.Background()); err != nil {
				w.WriteHeader(500)
				w.Write(fmt.Appendf(nil, "unable to execute transaction: %s", err))
				return
			}

			var html []byte
			if walletsCount == 0 {
				html = executeTemplateMust(templates, "wallets_table", nil)
			}

			w.WriteHeader(200)
			w.Write(html)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}
