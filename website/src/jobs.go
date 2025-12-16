package main

import (
	"fmt"
	"html/template"
	"net/http"
	"taxee/pkg/db"
	"taxee/pkg/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

func jobsHandler(pool *pgxpool.Pool, templates *template.Template) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userAccountId := 1
		// TODO: auth

		switch r.Method {
		case http.MethodGet:
		case http.MethodPut:
			if err := r.ParseMultipartForm(10 << 20); err != nil {
				w.WriteHeader(500)
				logger.Error("unable to parse form: %s", err.Error())
				return
			}

			jobType := r.Form.Get("type")
			if len(jobType) == 0 {
				w.WriteHeader(400)
				w.Write([]byte("type: missing"))
				return
			}
			switch jobType {
			case "parse_transactions", "parse_events":
			default:
				w.WriteHeader(400)
				w.Write(fmt.Appendf(nil, "type: %s is invalid", jobType))
				return
			}

			var newStatus db.Status
			if err := newStatus.ParseString(r.Form.Get("status")); err != nil {
				w.WriteHeader(400)
				w.Write(fmt.Appendf(nil, "status: %s", err.Error()))
				return
			}

			switch newStatus {
			case db.StatusQueued:
				const updateJobQuery = `
					insert into worker_job (
						user_account_id, type, status, queued_at
					) values (
						$1, $2, 'queued', now()
					) on conflict (user_account_id, type) do update set
						status = 'queued',
						queued_at = now()
					where
						worker_job.status in ('success', 'error', 'canceled')
					returning
						id
				`

				// var batch pgx.Batch
				// var status int
				// var errMessage string
				//
				// switch jobType {
				// case "parse_transactions":
				// 	// all wallets need be in terminal state (success, error, canceled, cancel_scheduled)
				// 	const selectNonTerminalWalletsQuery = `
				// 		select 1 from wallet where
				// 			user_account_id = $1 and
				// 			status in ('queued', 'in_progress')
				// 	`
				// 	batch.Queue(
				// 		selectNonTerminalWalletsQuery,
				// 		userAccountId,
				// 	).Query(func(rows pgx.Rows) error {
				// 		if rows.Next() {
				// 			status = 409
				// 			errMessage = "some wallets are still in queued / in_progress state"
				// 			return errors.New("")
				// 		}
				// 		return nil
				// 	})
				//
				// 	batch.Queue(
				// 		updateJobQuery,
				// 		userAccountId, jobType,
				// 	).QueryRow(func(row pgx.Row) error {
				// 		var jobId int32
				// 		if err := row.Scan(&jobId); err != nil {
				// 			status = 409
				// 			errMessage = "parse transactions in progress"
				// 			return fmt.Errorf("")
				// 		}
				// 		return nil
				// 	})
				// case "parse_events":
				// 	// parse transactions needs to be finished successfuly
				// 	const selectSuccessfulParseTxsJobQuery = `
				// 		select 1 from worker_job where
				// 			user_account_id = $1 and
				// 			type = 'parse_transactions' and
				// 			status = 'success'
				// 	`
				// 	batch.Queue(
				// 		selectSuccessfulParseTxsJobQuery,
				// 		userAccountId,
				// 	).Query(func(rows pgx.Rows) error {
				// 		if !rows.Next() {
				// 			// TODO: this sould be handled automatically
				// 			status = 409
				// 			errMessage = "parse_transactions needs to be done before parse_events"
				//
				// 		}
				// 	})
				// }

				row := pool.QueryRow(r.Context(), updateJobQuery, userAccountId, jobType)
				var jobId int32
				if err := row.Scan(&jobId); err != nil {
					w.WriteHeader(500)
					logger.Error("unable to update worker job: %s", err)
					return
				}

				w.WriteHeader(200)
				logger.Info("id:%d", jobId)

			case db.StatusCanceled:
			}

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}

}
