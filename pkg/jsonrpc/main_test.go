package jsonrpc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type mockValidator struct {
	validateErr error
	Value       string `json:"value"`
}

func (m *mockValidator) Validate() error {
	return m.validateErr
}

func TestLimiter_Wait(t *testing.T) {
	t.Run("immediate execution when bucket has capacity", func(t *testing.T) {
		lm := &limiter{
			bucket:      10,
			cuPerSecond: 10,
			lastUpdate:  time.Now(),
		}

		ctx := context.Background()
		start := time.Now()
		err := lm.wait(ctx, 10)
		elapsed := time.Since(start)

		require.NoError(t, err)
		require.Less(t, elapsed, 10*time.Millisecond, "expected immediate execution")
		require.Equal(t, 0.0, lm.bucket)
	})

	t.Run("wait when bucket insufficient", func(t *testing.T) {
		lm := &limiter{
			bucket:      5,
			cuPerSecond: 10,
			lastUpdate:  time.Now(),
		}

		ctx := context.Background()
		start := time.Now()
		err := lm.wait(ctx, 10)
		elapsed := time.Since(start)

		require.NoError(t, err)
		// Should wait ~500ms for 5 CUs at 10 CU/s
		require.GreaterOrEqual(t, elapsed, 400*time.Millisecond, "expected to wait")
	})

	t.Run("refill bucket over time", func(t *testing.T) {
		lm := &limiter{
			bucket:      5,
			cuPerSecond: 10,
			lastUpdate:  time.Now().Add(-2 * time.Second),
		}

		ctx := context.Background()
		err := lm.wait(ctx, 6)

		require.NoError(t, err)
		// Bucket should have refilled: 5 + (2s * 10 CU/s) = 10, then 10 - 6 = 4
		require.InDelta(t, 4.0, lm.bucket, 0.1)
	})

	t.Run("respect max capacity", func(t *testing.T) {
		lm := &limiter{
			bucket:      5,
			cuPerSecond: 10,
			lastUpdate:  time.Now().Add(-20 * time.Second),
		}

		ctx := context.Background()
		err := lm.wait(ctx, 10)

		require.NoError(t, err)
		require.LessOrEqual(t, lm.bucket, 10.0, "bucket exceeded max capacity")
	})

	t.Run("context cancellation", func(t *testing.T) {
		lm := &limiter{
			bucket:      0,
			cuPerSecond: 1,
			lastUpdate:  time.Now(),
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := lm.wait(ctx, 100)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestNewLimiter(t *testing.T) {
	t.Setenv("_", "_")

	oglm := lm

	cuPerSecond := 50
	methodsCosts := map[string]int{
		"eth_getBalance": 10,
		"eth_call":       20,
	}

	NewLimiter(cuPerSecond, methodsCosts)

	require.NotNil(t, lm, "limiter not initialized")
	require.Equal(t, float64(cuPerSecond), lm.cuPerSecond)
	require.Equal(t, lm.cuPerSecond, lm.bucket, "initial bucket should equal maxCap")

	lm = oglm
}

func TestCall(t *testing.T) {
	t.Run("successful call", func(t *testing.T) {
		t.Setenv("_", "_")
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req request
			json.NewDecoder(r.Body).Decode(&req)

			res := response{
				Id:     req.Id,
				Result: json.RawMessage(`{"value":"test"}`),
			}
			json.NewEncoder(w).Encode(res)
		}))
		defer server.Close()

		result := &mockValidator{}
		err := Call(context.Background(), server.URL, "test_method", nil, result, false)

		require.NoError(t, err)
		require.Equal(t, "test", result.Value)
	})

	t.Run("retry on null result", func(t *testing.T) {
		t.Setenv("_", "_")
		callCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req request
			json.NewDecoder(r.Body).Decode(&req)

			callCount++
			var result json.RawMessage
			if callCount == 1 {
			} else {
				result = json.RawMessage(`{"value":"success"}`)
			}

			res := response{
				Id:     req.Id,
				Result: result,
			}
			json.NewEncoder(w).Encode(res)
		}))
		defer server.Close()

		result := &mockValidator{}
		err := Call(context.Background(), server.URL, "test_method", nil, result, true)

		require.NoError(t, err)
		require.Equal(t, 2, callCount)
		require.Equal(t, "success", result.Value)
	})

	t.Run("max retries exceeded", func(t *testing.T) {
		t.Setenv("_", "_")
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req request
			json.NewDecoder(r.Body).Decode(&req)

			res := response{
				Id:     req.Id,
				Result: nil,
			}
			json.NewEncoder(w).Encode(res)
		}))
		defer server.Close()

		result := &mockValidator{}
		err := Call(context.Background(), server.URL, "test_method", nil, result, true)

		require.Error(t, err)
		require.ErrorIs(t, err, errMaxRetries)
	})

	t.Run("context cancellation", func(t *testing.T) {
		t.Setenv("_", "_")
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(100 * time.Millisecond)
		}))
		defer server.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		result := &mockValidator{}
		err := Call(ctx, server.URL, "test_method", nil, result, false)

		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("with rate limiter", func(t *testing.T) {
		t.Setenv("_", "_")
		NewLimiter(100, map[string]int{"test_method": 10})
		defer func() {
			mx.Lock()
			lm = nil
			mx.Unlock()
		}()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req request
			json.NewDecoder(r.Body).Decode(&req)

			res := response{
				Id:     req.Id,
				Result: json.RawMessage(`{"value":"limited"}`),
			}
			json.NewEncoder(w).Encode(res)
		}))
		defer server.Close()

		result := &mockValidator{}
		err := Call(context.Background(), server.URL, "test_method", nil, result, false)

		require.NoError(t, err)
		require.Equal(t, "limited", result.Value)
	})

	t.Run("validation error", func(t *testing.T) {
		t.Setenv("_", "_")
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req request
			json.NewDecoder(r.Body).Decode(&req)

			res := response{
				Id:     req.Id,
				Result: json.RawMessage(`{"value":"test"}`),
			}
			json.NewEncoder(w).Encode(res)
		}))
		defer server.Close()

		expectedErr := json.Unmarshal([]byte("invalid"), new(string))
		result := &mockValidator{validateErr: expectedErr}
		err := Call(context.Background(), server.URL, "test_method", nil, result, false)

		require.Error(t, err)
		require.Equal(t, expectedErr, err)
	})
}

func TestCallBatch(t *testing.T) {
	t.Run("successful batch call", func(t *testing.T) {
		t.Setenv("_", "_")
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var reqs []*request
			json.NewDecoder(r.Body).Decode(&reqs)

			responses := make([]*response, len(reqs))
			for i, req := range reqs {
				responses[i] = &response{
					Id:     req.Id,
					Result: json.RawMessage(`{"value":"test"}`),
				}
			}
			json.NewEncoder(w).Encode(responses)
		}))
		defer server.Close()

		batch := NewBatch(server.URL)
		callbackCount := 0

		for range 3 {
			batch.Queue("test_method", nil, func(result json.RawMessage) (bool, error) {
				callbackCount++
				return false, nil
			})
		}

		err := CallBatch(context.Background(), batch)

		require.NoError(t, err)
		require.Equal(t, 3, callbackCount)
	})

	t.Run("empty batch", func(t *testing.T) {
		t.Setenv("_", "_")
		batch := NewBatch("http://example.com")
		err := CallBatch(context.Background(), batch)

		require.NoError(t, err)
	})

	t.Run("retry in batch", func(t *testing.T) {
		t.Setenv("_", "_")
		callCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var reqs []*request
			json.NewDecoder(r.Body).Decode(&reqs)

			responses := make([]*response, len(reqs))
			for i, req := range reqs {
				responses[i] = &response{
					Id:     req.Id,
					Result: json.RawMessage(`{"value":"test"}`),
				}
			}
			json.NewEncoder(w).Encode(responses)
		}))
		defer server.Close()

		batch := NewBatch(server.URL)

		batch.Queue("test_method", nil, func(result json.RawMessage) (bool, error) {
			callCount++
			if callCount == 1 {
				return true, nil // retry
			}
			return false, nil
		})

		err := CallBatch(context.Background(), batch)

		require.NoError(t, err)
		require.Equal(t, 2, callCount)
	})

	t.Run("max retries in batch", func(t *testing.T) {
		t.Setenv("_", "_")
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var reqs []*request
			json.NewDecoder(r.Body).Decode(&reqs)

			responses := make([]*response, len(reqs))
			for i, req := range reqs {
				responses[i] = &response{
					Id:     req.Id,
					Result: json.RawMessage(`{"value":"test"}`),
				}
			}
			json.NewEncoder(w).Encode(responses)
		}))
		defer server.Close()

		batch := NewBatch(server.URL)

		batch.Queue("test_method", nil, func(result json.RawMessage) (bool, error) {
			return true, nil // always retry
		})

		err := CallBatch(context.Background(), batch)

		require.Error(t, err)
		require.ErrorIs(t, err, errMaxRetries)
	})

	t.Run("callback error", func(t *testing.T) {
		t.Setenv("_", "_")
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var reqs []*request
			json.NewDecoder(r.Body).Decode(&reqs)

			responses := make([]*response, len(reqs))
			for i, req := range reqs {
				responses[i] = &response{
					Id:     req.Id,
					Result: json.RawMessage(`{"value":"test"}`),
				}
			}
			json.NewEncoder(w).Encode(responses)
		}))
		defer server.Close()

		batch := NewBatch(server.URL)
		expectedErr := json.Unmarshal([]byte("invalid"), new(string))

		batch.Queue("test_method", nil, func(result json.RawMessage) (bool, error) {
			return false, expectedErr
		})

		err := CallBatch(context.Background(), batch)

		require.Error(t, err)
		require.Equal(t, expectedErr, err)
	})

	t.Run("with rate limiter", func(t *testing.T) {
		t.Setenv("_", "_")
		NewLimiter(100, map[string]int{"test_method": 10})
		defer func() {
			mx.Lock()
			lm = nil
			mx.Unlock()
		}()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var reqs []*request
			json.NewDecoder(r.Body).Decode(&reqs)

			responses := make([]*response, len(reqs))
			for i, req := range reqs {
				responses[i] = &response{
					Id:     req.Id,
					Result: json.RawMessage(`{"value":"limited"}`),
				}
			}
			json.NewEncoder(w).Encode(responses)
		}))
		defer server.Close()

		batch := NewBatch(server.URL)
		callbackCount := 0

		for range 2 {
			batch.Queue("test_method", nil, func(result json.RawMessage) (bool, error) {
				callbackCount++
				return false, nil
			})
		}

		err := CallBatch(context.Background(), batch)

		require.NoError(t, err)
		require.Equal(t, 2, callbackCount)
	})

	t.Run("context cancellation", func(t *testing.T) {
		t.Setenv("_", "_")
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(100 * time.Millisecond)
		}))
		defer server.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		batch := NewBatch(server.URL)
		batch.Queue("test_method", nil, func(result json.RawMessage) (bool, error) {
			return false, nil
		})

		err := CallBatch(ctx, batch)

		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("nil callback", func(t *testing.T) {
		t.Setenv("_", "_")
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var reqs []*request
			json.NewDecoder(r.Body).Decode(&reqs)

			responses := make([]*response, len(reqs))
			for i, req := range reqs {
				responses[i] = &response{
					Id:     req.Id,
					Result: json.RawMessage(`{"value":"test"}`),
				}
			}
			json.NewEncoder(w).Encode(responses)
		}))
		defer server.Close()

		batch := NewBatch(server.URL)
		batch.Queue("test_method", nil, nil)

		err := CallBatch(context.Background(), batch)

		require.NoError(t, err)
	})

	t.Run("chunking by max batch size", func(t *testing.T) {
		t.Setenv("_", "_")
		requestCounts := []int{}
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var reqs []*request
			json.NewDecoder(r.Body).Decode(&reqs)
			requestCounts = append(requestCounts, len(reqs))

			responses := make([]*response, len(reqs))
			for i, req := range reqs {
				responses[i] = &response{
					Id:     req.Id,
					Result: json.RawMessage(`{"value":"test"}`),
				}
			}
			json.NewEncoder(w).Encode(responses)
		}))
		defer server.Close()

		batch := NewBatch(server.URL)
		totalRequests := 1500
		callbackCount := 0

		for i := 0; i < totalRequests; i++ {
			batch.Queue("test_method", nil, func(result json.RawMessage) (bool, error) {
				callbackCount++
				return false, nil
			})
		}

		err := CallBatch(context.Background(), batch)

		require.NoError(t, err)
		require.Equal(t, totalRequests, callbackCount)
		require.GreaterOrEqual(t, len(requestCounts), 2, "should split into multiple batches")

		// Verify each batch respects maxAlchemyBatchSize
		for _, count := range requestCounts {
			require.LessOrEqual(t, count, maxAlchemyBatchSize)
		}
	})

	t.Run("chunking by rate limiter capacity", func(t *testing.T) {
		t.Setenv("_", "_")
		NewLimiter(50, map[string]int{"test_method": 10})
		defer func() {
			mx.Lock()
			lm = nil
			mx.Unlock()
		}()

		requestCounts := []int{}
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var reqs []*request
			json.NewDecoder(r.Body).Decode(&reqs)
			requestCounts = append(requestCounts, len(reqs))

			responses := make([]*response, len(reqs))
			for i, req := range reqs {
				responses[i] = &response{
					Id:     req.Id,
					Result: json.RawMessage(`{"value":"test"}`),
				}
			}
			json.NewEncoder(w).Encode(responses)
		}))
		defer server.Close()

		batch := NewBatch(server.URL)
		totalRequests := 10
		callbackCount := 0

		for i := 0; i < totalRequests; i++ {
			batch.Queue("test_method", nil, func(result json.RawMessage) (bool, error) {
				callbackCount++
				return false, nil
			})
		}

		err := CallBatch(context.Background(), batch)

		require.NoError(t, err)
		require.Equal(t, totalRequests, callbackCount)
		require.GreaterOrEqual(t, len(requestCounts), 2, "should split into multiple batches due to rate limit")

		// Verify each batch respects rate limiter capacity
		// maxCap = 50 * 10 = 500 CU, each request costs 10 CU, so max 50 requests per batch
		for _, count := range requestCounts {
			require.LessOrEqual(t, count, 50)
		}
	})

	t.Run("chunking with mixed method costs", func(t *testing.T) {
		t.Setenv("_", "_")
		NewLimiter(100, map[string]int{
			"cheap_method":     5,
			"expensive_method": 50,
		})
		defer func() {
			mx.Lock()
			lm = nil
			mx.Unlock()
		}()

		requestCounts := []int{}
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var reqs []*request
			json.NewDecoder(r.Body).Decode(&reqs)
			requestCounts = append(requestCounts, len(reqs))

			responses := make([]*response, len(reqs))
			for i, req := range reqs {
				responses[i] = &response{
					Id:     req.Id,
					Result: json.RawMessage(`{"value":"test"}`),
				}
			}
			json.NewEncoder(w).Encode(responses)
		}))
		defer server.Close()

		batch := NewBatch(server.URL)
		callbackCount := 0

		// Add 30 expensive methods (30 * 50 = 150 CU total)
		for range 3 {
			batch.Queue("expensive_method", nil, func(result json.RawMessage) (bool, error) {
				callbackCount++
				return false, nil
			})
		}

		// Add 50 cheap methods (50 * 5 = 25 CU total)
		for range 5 {
			batch.Queue("cheap_method", nil, func(result json.RawMessage) (bool, error) {
				callbackCount++
				return false, nil
			})
		}

		err := CallBatch(context.Background(), batch)

		require.NoError(t, err)
		require.Equal(t, 8, callbackCount)
		require.GreaterOrEqual(t, len(requestCounts), 2, "should split into multiple batches due to mixed costs")
	})

	t.Run("exact max batch size boundary", func(t *testing.T) {
		t.Setenv("_", "_")
		requestCounts := []int{}
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var reqs []*request
			json.NewDecoder(r.Body).Decode(&reqs)
			requestCounts = append(requestCounts, len(reqs))

			responses := make([]*response, len(reqs))
			for i, req := range reqs {
				responses[i] = &response{
					Id:     req.Id,
					Result: json.RawMessage(`{"value":"test"}`),
				}
			}
			json.NewEncoder(w).Encode(responses)
		}))
		defer server.Close()

		batch := NewBatch(server.URL)
		totalRequests := maxAlchemyBatchSize
		callbackCount := 0

		for range totalRequests {
			batch.Queue("test_method", nil, func(result json.RawMessage) (bool, error) {
				callbackCount++
				return false, nil
			})
		}

		err := CallBatch(context.Background(), batch)

		require.NoError(t, err)
		require.Equal(t, totalRequests, callbackCount)
		require.Equal(t, 1, len(requestCounts), "should be exactly one batch")
		require.Equal(t, maxAlchemyBatchSize, requestCounts[0])
	})
}

