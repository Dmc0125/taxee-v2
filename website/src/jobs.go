package main

import (
	"context"
	"html/template"
	"net/http"
	"taxee/pkg/db"
	"taxee/pkg/logger"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

func jobsSseHandler(pool *pgxpool.Pool, templates *template.Template) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userAccountId := 1
		// TODO: auth

		_ = userAccountId

		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		flusher, ok := initSSEHandler(w)
		if !ok {
			return
		}

		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		prevStatusData := &uiStatusIndicatorData{}

		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
			}

			var jobs uiJobs

			err := db.ExecuteTx(r.Context(), pool, func(ctx context.Context, tx pgx.Tx) error {
				const selectWallets = `
					select label, status, address from wallet where
						user_account_id = $1 and
						delete_scheduled = false
				`
				rows, err := tx.Query(ctx, selectWallets, userAccountId)
				if err != nil {
					return err
				}
				for rows.Next() {
					var label pgtype.Text
					var status db.Status
					var address string
					if err := rows.Scan(&label, &status, &address); err != nil {
						return err
					}
					if !label.Valid {
						label.String = defaultWalletLabel(address)
					}
					jobs = append(jobs, &uiJob{
						status: status,
						label:  label.String,
						t:      db.JobFetchWallet,
					})
				}
				if err := rows.Err(); err != nil {
					return err
				}

				rows, err = tx.Query(ctx, selectJobsQuery, userAccountId)
				if err != nil {
					return err
				}
				for rows.Next() {
					if err := jobs.appendFromRows(rows); err != nil {
						return err
					}
				}
				if err := rows.Err(); err != nil {
					return err
				}

				return nil
			})

			if err != nil {
				logger.Error(err.Error())
				sendSSEError(w, flusher, "")
				return
			}

			statusData := newUiStatusIndicatorData(jobs)

			if !prevStatusData.eq(statusData) {
				html := executeTemplateMust(templates, "global_status", statusData)
				sendSSEUpdate(w, flusher, string(html))
			}
			prevStatusData = statusData

			switch statusData.Status {
			case uiIndicatorStatusSuccess, uiIndicatorStatusError, uiIndicatorStatusUninitialized:
				sendSSEClose(w, flusher)
				return
			}
		}
	}
}
