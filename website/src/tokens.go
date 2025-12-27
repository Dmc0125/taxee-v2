package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"taxee/pkg/assert"
	"taxee/pkg/coingecko"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func tokensHandler(
	pool *pgxpool.Pool,
	templates *template.Template,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(400)
			w.Write([]byte("invalid method"))
			return
		}

		flusher, ok := initSSEHandler(w)
		if !ok {
			return
		}

		query := r.URL.Query()
		tokensQueryParam := query.Get("tokens")

		fetched := make([]string, 0)
		batch := pgx.Batch{}

		const setImgUrlQuery = `
			update coingecko_token_data set
				image_url = $1
			where
				coingecko_id = $2
		`

		for coingeckoId := range strings.SplitSeq(tokensQueryParam, ",") {
			if slices.Contains(fetched, coingeckoId) {
				continue
			}
			fetched = append(fetched, coingeckoId)

			metadata, err := coingecko.GetCoinMetadata(r.Context(), coingeckoId)
			if err != nil {
				sendSSEError(w, flusher, coingeckoId)
				return
			}

			imgUrl, err := url.PathUnescape(metadata.Image.Small)
			batch.Queue(setImgUrlQuery, imgUrl, coingeckoId)

			component := executeTemplateMust(
				templates,
				"event_token_img",
				eventTokenImgComponentData{
					ImgUrl: imgUrl,
				},
			)
			encoded := hex.EncodeToString(component)

			sendSSEUpdate(w, flusher, fmt.Sprintf("%s,%s", coingeckoId, encoded))
		}

		if len(fetched) > 0 {
			br := pool.SendBatch(context.Background(), &batch)
			// NOTE: ignore the error, it doesn't really matter, just try next time
			err := br.Close()
			assert.NoErr(err, "unable to set img urls")
		}

		sendSSEClose(w, flusher)
	}
}
