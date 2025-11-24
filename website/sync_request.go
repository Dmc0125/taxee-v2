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

func sendError(
	w http.ResponseWriter,
	flusher http.Flusher,
	msg string,
	args ...any,
) {
	m := fmt.Sprintf(msg, args...)
	fmt.Fprintf(w, "event: error\ndata: %s\n\n", m)
	flusher.Flush()
}

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
			headers := w.Header()
			headers.Set("content-type", "text/event-stream")
			headers.Set("cache-control", "no-cache")
			headers.Set("connection", "keep-alive")

			flusher, ok := w.(http.Flusher)
			if !ok {
				http.Error(w, "streaming not supported", 500)
				return
			}

			urlQuery := r.URL.Query()
			requestIdRaw := urlQuery.Get("id")
			var requestId int32

			if ri, err := strconv.ParseInt(requestIdRaw, 10, 32); err != nil {
				sendError(w, flusher, "invalid request id: %s", requestIdRaw)
				return
			} else {
				if ri < 0 {
					sendError(w, flusher, "request id can not be < 0")
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
					var status string

					if err := row.Scan(&syncRequestStatus); err != nil {
						if !errors.Is(err, pgx.ErrNoRows) {
							sendError(w, flusher, "unable to query sync request")
							return
						}
						// TODO: when done, should render events
						progressIndicator.Done = true
						status = "done"
					} else {
						progressIndicator.Status = syncRequestStatus.String()
					}

					if prevSyncRequestStatus == syncRequestStatus && status != "done" {
						continue
					}
					prevSyncRequestStatus = syncRequestStatus

					templateBuf := executeTemplateMust(
						templates,
						"events_progress_indicator",
						progressIndicator,
					)

					encoded := hex.EncodeToString(templateBuf)

					fmt.Fprintf(w, "event: update\ndata: %s,%s\n\n", status, encoded)
					flusher.Flush()

					if status == "done" {
						return
					}
				}
			}
		}
	}
}
