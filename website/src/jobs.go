package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"taxee/pkg/assert"
	"taxee/pkg/db"
	"time"
	"unsafe"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type paramGetter interface {
	Get(string) string
}

func parseIntParam[T any](
	w http.ResponseWriter,
	getter paramGetter,
	result *T,
	key string,
	positiveOnly bool,
	size int,
) bool {
	value := getter.Get(key)

	if value == "" {
		w.WriteHeader(400)
		w.Write(fmt.Appendf(nil, "%s: missing", key))
		return false
	}

	if v, err := strconv.ParseInt(value, 10, size); err == nil {
		if positiveOnly && v < 0 {
			w.WriteHeader(400)
			w.Write(fmt.Appendf(nil, "%s: must be positive", key))
			return false
		}
		*result = *(*T)(unsafe.Pointer(&v))
		return true
	} else {
		w.WriteHeader(400)
		w.Write(fmt.Appendf(nil, "%s: %s", key, err.Error()))
		return false
	}
}

func parseUintParam[T any](
	w http.ResponseWriter,
	getter paramGetter,
	result *T,
	key string,
	size int,
) bool {
	value := getter.Get(key)

	if value == "" {
		w.WriteHeader(400)
		w.Write(fmt.Appendf(nil, "%s: missing", key))
		return false
	}

	if v, err := strconv.ParseUint(value, 10, size); err == nil {
		*result = *(*T)(unsafe.Pointer(&v))
		return true
	} else {
		w.WriteHeader(400)
		w.Write(fmt.Appendf(nil, "%s: %s", key, err.Error()))
		return false
	}
}

func jobsHandle(pool *pgxpool.Pool, templates *template.Template) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userAccountId := 1

		switch r.Method {
		case http.MethodGet:
			flusher, ok := initSSEHandler(w)
			if !ok {
				return
			}

			jobId, err := uuid.Parse(r.URL.Query().Get("id"))
			if err != nil {
				sendSSEError(w, flusher, fmt.Sprintf("id: %s", err))
				return
			}

			ticker := time.NewTicker(time.Second)
			oldStatus := uint8(255)

			for {
				select {
				case <-r.Context().Done():
					return
				case <-ticker.C:
				}

				const selectStatusQuery = `
					select type, status, data from worker_job where id = $1
				`
				row := pool.QueryRow(r.Context(), selectStatusQuery, jobId)
				var jobType uint8
				var status uint8
				var dataSerialized json.RawMessage
				if err := row.Scan(&jobType, &status, &dataSerialized); err != nil {
					if errors.Is(err, pgx.ErrNoRows) {
						sendSSEError(w, flusher, "job not found")
						return
					}
					sendSSEError(w, flusher, fmt.Sprintf("unable to query job: %s", err.Error()))
					return
				}

				if status == oldStatus {
					continue
				}

				switch jobType {
				case db.WorkerJobFetchWallet:
					var data db.WorkerJobFetchWalletData
					if err := json.Unmarshal(dataSerialized, &data); err != nil {
						sendSSEError(w, flusher, fmt.Sprintf("unable to unmarshal data: %s", err))
						return
					}

					html := executeTemplateMust(templates, "wallet", newWalletComponent(
						data.WalletAddress,
						data.WalletId,
						data.Network,
						status,
						jobId,
					))
					sendSSEUpdate(w, flusher, string(html))
				}

				switch status {
				case db.WorkerJobSuccess, db.WorkerJobError, db.WorkerJobCanceled:
					sendSSEClose(w, flusher)
					return
				}

				oldStatus = status
			}
		case http.MethodPut:
			err := r.ParseMultipartForm(10 << 20)
			if err != nil {
				w.WriteHeader(500)
				w.Write(fmt.Appendf(nil, "unable to parse form data: %s", err))
				return
			}

			jobId, err := uuid.Parse(r.Form.Get("id"))
			if err != nil {
				w.WriteHeader(400)
				w.Write(fmt.Appendf(nil, "id: %s", err))
				return
			}

			var newStatus uint8
			if ok := parseUintParam(w, paramGetter(r.Form), &newStatus, "status", 8); !ok {
				return
			}

			switch newStatus {
			case 0:
				// put job to queued
				// old status must be
				// - done, error, canceled
				tx, err := pool.Begin(r.Context())
				if err != nil {
					w.WriteHeader(500)
					w.Write(fmt.Appendf(nil, "unable to begin tx: %s", err))
					return
				}
				defer tx.Rollback(r.Context())

				const setJobAsQueuedQuery = `
					update worker_job set
						status = 0
					where
						id = $1 and 
						user_account_id = $2 and
						(status = 1 or status = 2 or status = 3 or status = 5)
					returning
						type, data
				`
				row := tx.QueryRow(r.Context(), setJobAsQueuedQuery, jobId, userAccountId)
				var jobType uint8
				var dataSerialized json.RawMessage
				if err := row.Scan(&jobType, &dataSerialized); err != nil {
					if errors.Is(err, pgx.ErrNoRows) {
						w.WriteHeader(404)
					} else {
						w.WriteHeader(500)
						w.Write(fmt.Appendf(nil, "unable to update job: %s", err))
					}
					return
				}

				switch jobType {
				case db.WorkerJobFetchWallet:
					var data db.WorkerJobFetchWalletData
					if err := json.Unmarshal(dataSerialized, &data); err != nil {
						w.WriteHeader(500)
						w.Write(fmt.Appendf(nil, "unable to deserialize worker data: %s", err))
						return
					}

					const selectWalletQuery = `
						select delete_scheduled from wallet where
							id = $1 and user_account_id = $2
					`
					row := tx.QueryRow(r.Context(), selectWalletQuery, data.WalletId, userAccountId)
					var deleteScheduled bool
					if err := row.Scan(&deleteScheduled); err != nil {
						w.WriteHeader(500)
						w.Write(fmt.Appendf(nil, "unable to select wallet: %s", err))
						return
					}

					if deleteScheduled {
						w.WriteHeader(409)
						return
					}

					if err := tx.Commit(r.Context()); err != nil {
						w.WriteHeader(500)
						w.Write(fmt.Appendf(nil, "unable to commit tx: %s", err))
						return
					}

					w.Header().Add("location", fmt.Sprintf("/jobs?id=%s", jobId))
					w.WriteHeader(202)

					walletData := newWalletComponent(
						data.WalletAddress,
						data.WalletId,
						data.Network,
						0,
						jobId,
					)
					html := executeTemplateMust(templates, "wallet", walletData)
					w.Write(html)
				default:
					assert.True(false, "unimplemented")
				}
			case 4:
				// NOTE: job is set to scheduled for cancel when wallet is
				// scheduled for delete, so no need to check wallet here
				const setJobAsScheduledForCancelQuery = `
					update worker_job set
						status = (
							case
								when status = $3::smallint then $4::smallint
								when status = $5::smallint then $6::smallint
							end
						)
					where
						id = $1 and user_account_id = $2 and
						(status = $3::smallint or status = $5::smallint)
					returning
						old.status, data
				`
				row := pool.QueryRow(
					r.Context(), setJobAsScheduledForCancelQuery,
					jobId, userAccountId,
					db.WorkerJobInProgress, db.WorkerJobCancelScheduled,
					db.WorkerJobQueued, db.WorkerJobCanceled,
				)
				var oldStatus uint8
				var dataSerialized json.RawMessage
				if err := row.Scan(&oldStatus, &dataSerialized); err != nil {
					if errors.Is(err, pgx.ErrNoRows) {
						w.WriteHeader(404)
						return
					}
					w.WriteHeader(500)
					w.Write(fmt.Appendf(nil, "unable to update job: %s", err.Error()))
					return
				}

				switch oldStatus {
				case db.WorkerJobQueued:
					var data db.WorkerJobFetchWalletData
					if err := json.Unmarshal(dataSerialized, &data); err != nil {
						w.WriteHeader(500)
						w.Write(fmt.Appendf(nil, "unable to unmarshal data: %s", err))
						return
					}

					w.WriteHeader(200)
					html := executeTemplateMust(templates, "wallet", newWalletComponent(
						data.WalletAddress,
						data.WalletId,
						data.Network,
						db.WorkerJobCanceled,
						jobId,
					))
					w.Write(html)
				case db.WorkerJobInProgress:
					w.Header().Add("location", fmt.Sprintf("/job/%s", jobId))
					w.WriteHeader(202)
				}
			default:
				w.WriteHeader(400)
			}
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}

}
