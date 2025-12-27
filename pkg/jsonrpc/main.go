package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	apiutils "taxee/pkg/api_utils"
	"taxee/pkg/assert"
	"time"
)

var errMaxRetries = errors.New("maxRetries")

var id atomic.Uint64
var mx sync.Mutex
var lm *limiter

const (
	maxAlchemyBatchSize = 1000
)

type limiter struct {
	bucket       float64
	cuPerSecond  float64
	lastUpdate   time.Time
	methodsCosts map[string]int
	batchCost    float64
}

func (lm *limiter) wait(ctx context.Context, cost float64) error {
	if lm.bucket < cost {
		// refill
		now := time.Now()
		elapsed := now.Sub(lm.lastUpdate).Seconds()
		lm.bucket += elapsed * float64(lm.cuPerSecond)
		if lm.bucket > lm.cuPerSecond {
			lm.bucket = lm.cuPerSecond
		}
		lm.lastUpdate = now

		// wait
		if lm.bucket < cost {
			missing := cost - lm.bucket
			waitSeconds := missing / lm.cuPerSecond
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				time.Sleep(time.Duration(waitSeconds*1000) * time.Millisecond)
			}
		}
	}
	lm.bucket -= cost
	return nil
}

func NewLimiter(cuPerSecond int, methodsCosts ...map[string]int) {
	mx.Lock()
	defer mx.Unlock()
	lm = &limiter{
		bucket:       float64(cuPerSecond),
		cuPerSecond:  float64(cuPerSecond),
		methodsCosts: make(map[string]int),
	}

	for _, mc := range methodsCosts {
		maps.Copy(lm.methodsCosts, mc)
	}
}

type request struct {
	Jsonrpc string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
	Id      uint64 `json:"id"`
}

type responseError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data"`
}

type response struct {
	Id     uint64          `json:"id"`
	Error  *responseError  `json:"error"`
	Result json.RawMessage `json:"result,omitempty"`
}

type Validator interface {
	Validate() error
}

func Call(
	ctx context.Context,
	url, method string,
	params any,
	result Validator,
	retry bool,
) error {
	retries, ok := ctx.Value("jsonrpc_retries").(int)
	// max 5 retries
	if ok && retries > 4 {
		return fmt.Errorf("%w: failed to call %s", errMaxRetries, method)
	}

	httpBody, err := json.Marshal(request{
		Jsonrpc: "2.0",
		Method:  method,
		Id:      id.Add(1),
		Params:  params,
	})
	if err != nil {
		return fmt.Errorf("unable to marshal request: %w", err)
	}

	mx.Lock()

	if lm != nil {
		if err := lm.wait(ctx, float64(lm.methodsCosts[method])); err != nil {
			mx.Unlock()
			return err
		}
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(httpBody))
	if err != nil {
		return fmt.Errorf("unable to create request: %w", err)
	}
	req.Header.Add("content-type", "application/json")

	var res response
	statusCode, statusOk, err := apiutils.HttpSend(ctx, req, &res)
	mx.Unlock()

	if err != nil {
		return err
	}
	if res.Error != nil {
		assert.True(false, "TODO: handle jsonrpc errors")
	}
	if !statusOk {
		assert.True(false, "TODO: handle status not ok: %d %v", statusCode, res)
	}

	if len(res.Result) == 0 && retry {
		return Call(
			context.WithValue(ctx, "jsonrpc_retries", retries+1),
			url, method, params, result, true,
		)
	}

	if err := json.Unmarshal(res.Result, result); err != nil {
		return fmt.Errorf("unable to unmarshal response: %w\nBody: %s", err, string(res.Result))
	}

	return result.Validate()
}

type BatchCallback func(json.RawMessage) (retry bool, err error)

type batchRequest struct {
	*request
	cb      BatchCallback
	retries int
}

type Batch struct {
	url   string
	queue map[uint64]*batchRequest
}

func NewBatch(url string) *Batch {
	return &Batch{
		url:   url,
		queue: make(map[uint64]*batchRequest),
	}
}

func (b *Batch) Len() int {
	return len(b.queue)
}

func (b *Batch) Queue(method string, params any, callback BatchCallback) {
	rid := id.Add(1)

	b.queue[rid] = &batchRequest{
		request: &request{
			Jsonrpc: "2.0",
			Method:  method,
			Params:  params,
			Id:      rid,
		},
		cb: callback,
	}
}

func CallBatch(ctx context.Context, batch *Batch) error {
	if len(batch.queue) == 0 {
		return nil
	}

	mx.Lock()

	// calc total batch CUs
	batchChunk := make([]*request, 0)
	batchCus := float64(0)

	for _, br := range batch.queue {
		if len(batchChunk) == maxAlchemyBatchSize {
			break
		}

		if lm != nil {
			cost := float64(lm.methodsCosts[br.Method])
			if cost+batchCus > float64(lm.cuPerSecond) {
				break
			}
			batchCus += cost
		}

		batchChunk = append(batchChunk, br.request)
	}

	// wait
	if lm != nil {
		if err := lm.wait(ctx, batchCus); err != nil {
			mx.Unlock()
			return err
		}
	}

	body, err := json.Marshal(batchChunk)
	if err != nil {
		mx.Unlock()
		return fmt.Errorf("unable to marshal requests: %w", err)
	}

	req, err := http.NewRequest("POST", batch.url, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("unable to create request: %w", err)
	}
	req.Header.Add("content-type", "application/json")

	var responses []*response
	statusCode, statusOk, err := apiutils.HttpSend(ctx, req, &responses)
	mx.Unlock()

	if err != nil {
		return err
	}
	if !statusOk {
		assert.True(false, "unreachable: %d", statusCode)
	}

	slices.SortFunc(responses, func(r1, r2 *response) int {
		return int(r1.Id) - int(r2.Id)
	})

	for _, response := range responses {
		if response.Error != nil {
			assert.True(false, "TODO: handle jsonrpc errors: %v", *response.Error)
		}

		rid := response.Id
		br := batch.queue[rid]
		if br.cb != nil {
			retry, err := br.cb(response.Result)
			if err != nil {
				return err
			}

			if retry {
				if br.retries > 4 {
					return fmt.Errorf(
						"%w: unable to send batch: request id %d",
						errMaxRetries, response.Id,
					)
				}

				br.retries += 1
				continue
			}
		}

		delete(batch.queue, rid)
	}

	if len(batch.queue) > 0 {
		return CallBatch(ctx, batch)
	}

	return nil
}
