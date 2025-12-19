package main

import (
	"context"
	"html/template"
	"net/http"
	"taxee/pkg/db"
	"taxee/pkg/logger"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func parserSseHandler(pool *pgxpool.Pool, templates *template.Template) http.HandlerFunc {
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

		var prevNavbarStatus cmpNavbarStatus

		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
			}

			var navbarStatus cmpNavbarStatus

			err := db.ExecuteTx(r.Context(), pool, func(ctx context.Context, tx pgx.Tx) error {
				const selectWallets = `
					select label, status, address from wallet where
						user_account_id = $1 and
						status != 'delete'
				`
				rows, err := tx.Query(ctx, selectWallets, userAccountId)
				if err != nil {
					return err
				}
				for rows.Next() {
					var label, address string
					var status db.WalletStatus
					if err := rows.Scan(&label, &status, &address); err != nil {
						return err
					}
					navbarStatus.appendWallet(label, status)
				}
				if err := rows.Err(); err != nil {
					return err
				}

				const selectParser = `
					select status from parser
						where user_account_id = $1
				`
				row := tx.QueryRow(ctx, selectParser, userAccountId)
				if err := navbarStatus.appendParserFromRow(row); err != nil {
					return err
				}

				return nil
			})

			if err != nil {
				logger.Error(err.Error())
				sendSSEError(w, flusher, "")
				return
			}

			if !prevNavbarStatus.eq(&navbarStatus) {
				sendSSEUpdate(w, flusher, string(executeTemplateMust(
					templates, "navbar_status", navbarStatus,
				)))
				prevNavbarStatus = navbarStatus
			}

			switch navbarStatus.Status {
			case navbarStatusSuccess, navbarStatusUninit, navbarStatusError:
				sendSSEClose(w, flusher)
				return
			}
		}
	}
}
