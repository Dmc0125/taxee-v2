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
			case "parse_transactions":
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
