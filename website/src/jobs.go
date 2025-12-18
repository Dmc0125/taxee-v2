package main

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"os/user"
	"taxee/pkg/db"
	"taxee/pkg/logger"

	"github.com/jackc/pgx/v5"
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

			var jobType db.JobType
			if err := jobType.ParseString(r.Form.Get("type")); err != nil {
				w.WriteHeader(400)
				w.Write(fmt.Appendf(nil, "type: %s", err.Error()))
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
				var responseStatus int

				// TODO: when PT gets queued PE also has to get queued and PE
				// should only be allowed to be queued if PT was succes
				switch jobType {
				case db.JobParseTransactions:
					err := db.ExecuteTx(r.Context(), pool, func(ctx context.Context, tx pgx.Tx) error {
						const selectNonTerminalWallets = `
							select 1 from wallet where
								status in ('queued', 'in_progress') and
								user_account_id = $1 and
								delete_scheduled = false
						`
						var _d int
						err := tx.QueryRow(
							ctx, selectNonTerminalWallets,
							userAccountId,
						).Scan(&_d)
						if err != nil {
							if errors.Is(err, pgx.ErrNoRows) {
								err = nil
							} else {
								responseStatus = 500
								return fmt.Errorf("unable to query wallets: %w", err)
							}
						} else {
							responseStatus = 409
							return errors.New("")
						}

						const queueParseTransactions = `
							update worker_job set
								status = 'queued',
								queued_at = now()
							where
								status in ('success', 'error', 'canceled') and
								user_account_id = $1 and
								type = $2
							returning
								id
						`
						var jobId int32
						err = tx.QueryRow(
							ctx, queueParseTransactions,
							userAccountId, jobType,
						).Scan(&jobId)
						if err != nil {
							if errors.Is(err, pgx.ErrNoRows) {
								responseStatus = 404
							} else {
								responseStatus = 500
							}
							return fmt.Errorf("unable to update job: %w", err)
						}

						const queueParseEvents = `
							update worker_job set
								status = (
									case
										when status in ('success', 'error', 'canceled') then 'queued'
										else 'reset_scheduled'
									end
								)::status,
								queued_at = now()
							where
								user_account_id = $1 and
								type = $2 and
								status != 'queued'
						`
						_, err = tx.Exec(ctx, queueParseEvents, userAccountId, jobType)
						if err != nil {
							responseStatus = 500
							return fmt.Errorf("unable to update parse_events jobs: %w", err)
						}
						return nil
					})

					if err != nil {
						logger.Error(err.Error())
						w.WriteHeader(responseStatus)
						return
					}

					w.WriteHeader(202)
				case db.JobParseEvents:
					var responseStatus int

					db.ExecuteTx(r.Context(), pool, func(ctx context.Context, tx pgx.Tx) error {
						const queryParseTransactionsJob = `
							select 1 from worker_job where
								type = "parse_transactions" and
								status = 'success' and
								user_account_id = $1
						`
						var _d int
						err := tx.QueryRow(
							ctx, queryParseTransactionsJob, userAccountId,
						).Scan(&_d)
						switch {
						case errors.Is(err, pgx.ErrNoRows):
							responseStatus = 409
							return err
						case err != nil:
							responseStatus = 500
							logger.Error("unable to query job: %w", err)
							return err
						}

						const queueParseEventsJob = `
							update worker_job set
								status = (
									case
										when status = 'cancel_scheduled' then 'reset_scheduled'
										else 'queued'
								)::status,
								queued_at = now()
							where
								user_account_id = $1 and
								type = 'parse_events' and
								status in ('success', 'error', 'canceled', 'cancel_scheduled')
						`
						_, err = tx.Exec(ctx, queueParseEventsJob, userAccountId)
						if err != nil {
							responseStatus = 500
							logger.Error("unable to update job: %w", err)
							return err
						}

						return nil
					})

					if responseStatus != 0 {
						w.WriteHeader(responseStatus)
						return
					}

					w.WriteHeader(202)
				}
			case db.StatusCanceled:
			}

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}

}
