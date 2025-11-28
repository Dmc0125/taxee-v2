package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"taxee/pkg/db"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// -- no requests being processed
// request comes in
// insert request
//
// server pulls request
//
// server is done, server sets request to done

// -- request being processed
// request comes in
// insert request
// insert cancel request
//
// server pulls cancel request
// server stop, deletes previous request
//
// server pulls new request

func syncRequestHandler(
	pool *pgxpool.Pool,
	templates *template.Template,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// TODO auth
		userAccountId := 1

		switch r.Method {
		case "POST":
			urlQuery := r.URL.Query()
			requestTypeRaw := urlQuery.Get("type")
			var requestType db.SyncRequestType

			if rt, err := strconv.ParseInt(requestTypeRaw, 10, 8); err != nil {
				http.Error(
					w, fmt.Sprintf("invalid request type: %s", requestTypeRaw),
					400,
				)
				return
			} else {
				if rt < 0 && rt > 2 {
					http.Error(w, fmt.Sprintf("invalid request type: %d", rt), 400)
					return
				}
				requestType = db.SyncRequestType(rt)
			}

			const deleteSyncRequestsQuery = `
				delete from sync_request where user_account_id = $1
			`
			const insertSyncRequestQuery = `
				insert into sync_request (
					user_account_id, type, status
				) values (
					$1, $2, 0
				) returning id
			`

			var syncRequestId int32

			batch := pgx.Batch{}
			batch.Queue(deleteSyncRequestsQuery, userAccountId)
			q := batch.Queue(insertSyncRequestQuery, userAccountId, requestType)
			q.QueryRow(func(row pgx.Row) error {
				return row.Scan(&syncRequestId)
			})

			br := pool.SendBatch(context.Background(), &batch)
			if err := br.Close(); err != nil {
				http.Error(
					w, fmt.Sprintf("unable to create sync request: %s", err),
					500,
				)
				return
			}

			s := db.SyncRequestQueued
			templateBuf := executeTemplateMust(
				templates,
				"events_progress_indicator",
				eventsProgressIndicatorComponentData{
					Done:   false,
					Status: s.String(),
				},
			)

			w.WriteHeader(200)
			fmt.Fprintf(w, "%d,%s", syncRequestId, string(templateBuf))
		case "GET":
			flusher, ok := initSSEHandler(w)
			if !ok {
				return
			}

			urlQuery := r.URL.Query()
			requestIdRaw := urlQuery.Get("id")
			var requestId int32

			if ri, err := strconv.ParseInt(requestIdRaw, 10, 32); err != nil {
				sendSSEError(w, flusher, fmt.Sprintf("invalid request id: %s", requestIdRaw))
				return
			} else {
				if ri < 0 {
					sendSSEError(w, flusher, "request id can not be < 0")
					return
				}
				requestId = int32(ri)
			}

			clientDone := r.Context().Done()
			ticker := time.NewTicker(1 * time.Second)
			var prevSyncRequestStatus db.SyncRequestStatus

			for {
				select {
				case <-clientDone:
					return
				case <-ticker.C:
					const getSyncRequestQuery = `
						select status from sync_request where id = $1
					`
					row := pool.QueryRow(
						context.Background(), getSyncRequestQuery, requestId,
					)

					var syncRequestStatus db.SyncRequestStatus
					var progressIndicator eventsProgressIndicatorComponentData

					if err := row.Scan(&syncRequestStatus); err != nil {
						if !errors.Is(err, pgx.ErrNoRows) {
							sendSSEError(w, flusher, "unable to query sync request")
							return
						}
						progressIndicator.Done = true
					} else {
						progressIndicator.Status = syncRequestStatus.String()
					}

					if prevSyncRequestStatus == syncRequestStatus && !progressIndicator.Done {
						continue
					}
					prevSyncRequestStatus = syncRequestStatus

					templateBuf := executeTemplateMust(
						templates,
						"events_progress_indicator",
						progressIndicator,
					)
					encoded := hex.EncodeToString(templateBuf)
					sendSSEUpdate(w, flusher, encoded)

					if progressIndicator.Done {
						sendSSEClose(w, flusher)
						return
					}
				}
			}
		}
	}
}
