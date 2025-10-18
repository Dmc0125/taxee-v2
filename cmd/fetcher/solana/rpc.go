package solana

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"taxee/pkg/assert"
	"time"
)

const reqTimeout = 200

type ResponseError struct {
	Code    int
	Message string
	Data    any
}

type responseInternal[T any] struct {
	Jsonrpc string
	Id      uint64
	Result  T
	Error   *ResponseError
}

type Response[T any] struct {
	Id     uint64
	Result T
	Error  *ResponseError
}

type request struct {
	Jsonrpc string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  []any  `json:"params,omitempty"`
	Id      uint64 `json:"id"`
}

type Commitment string

const (
	CommitmentProcessed Commitment = "processed"
	CommitmentConfirmed Commitment = "confirmed"
	CommitmentFinalized Commitment = "finalized"
)

type Rpc struct {
	reqId        atomic.Uint64
	url          string
	nextReqMinTs time.Time
	mx           *sync.Mutex
}

func NewRpc() *Rpc {
	url := os.Getenv("SOLANA_RPC_URL")
	assert.True(url != "", "missing SOLANA_RPC_URL")

	return &Rpc{
		url: url,
		mx:  &sync.Mutex{},
	}
}

func (rpc *Rpc) newRequest(method string, params []any) *request {
	reqId := rpc.reqId.Load()
	rpc.reqId.Store(reqId + 1)
	return &request{
		Jsonrpc: "2.0",
		Method:  method,
		Params:  params,
		Id:      reqId,
	}
}

func (rpc *Rpc) sendRequest(reqData any, resData any) error {
	reqBody, err := json.Marshal(reqData)
	if err != nil {
		return fmt.Errorf("unable to marshal body: %s", err)
	}

	rpc.mx.Lock()
	ts := time.Now()
	if ts.UnixMilli() < rpc.nextReqMinTs.UnixMilli() {
		remainder := rpc.nextReqMinTs.Sub(ts)
		time.Sleep(remainder)
	}

	res, err := http.Post(rpc.url, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("unable to execute request: %s", err)
	}
	defer res.Body.Close()

	rpc.nextReqMinTs = time.UnixMilli(time.Now().UnixMilli() + reqTimeout)
	rpc.mx.Unlock()

	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("unable to read body: %s", err)
	}
	if res.StatusCode != 200 {
		return fmt.Errorf(
			"response status != 200\nBody: %s",
			string(resBody),
		)
	}

	if err := json.Unmarshal(resBody, resData); err != nil {
		return fmt.Errorf(
			"unable to unmarshal response: %w\nBody: %s",
			err,
			string(resBody),
		)
	}

	return nil
}

type GetSignaturesForAddressParams struct {
	Commitment Commitment `json:"commitment"`
	Limit      uint16     `json:"limit,omitempty"`
	Before     string     `json:"before,omitempty"`
	Until      string     `json:"until,omitempty"`
}

type GetSignaturesForAddressResponse struct {
	Signature          string
	Slot               uint64
	Err                any
	Memo               string
	BlockTime          int64
	ConfirmationStatus Commitment
}

func (rpc *Rpc) GetSignaturesForAddress(
	address string,
	params *GetSignaturesForAddressParams,
) (*Response[[]*GetSignaturesForAddressResponse], error) {
	p := new(GetSignaturesForAddressParams)
	if params == nil {
		p.Commitment = CommitmentFinalized
	} else {
		p = params
	}
	req := rpc.newRequest("getSignaturesForAddress", []any{address, p})

	resInternal := new(responseInternal[[]*GetSignaturesForAddressResponse])
	err := rpc.sendRequest(req, resInternal)

	if err == nil {
		return &Response[[]*GetSignaturesForAddressResponse]{
			Result: resInternal.Result,
			Error:  resInternal.Error,
		}, nil
	}

	return nil, err
}

type GetMultipleTransactionsParams struct {
	Signature  string
	Commitment Commitment
}

type TransactionLoadedAddress struct {
	Writable []string
	Readonly []string
}

type TransactionUiTokenAmount struct {
	Amount   string
	Decimals uint8
}

type TransactionTokenBalance struct {
	AccountIndex  uint8
	Mint          string
	Owner         string
	ProgramId     string
	UiTokenAmount *TransactionUiTokenAmount
}

type CompiledInnerInstruction struct {
	ProgramIdIndex uint8
	Accounts       []uint8
	Data           string
}

type TransactionInnerInstructions struct {
	Index        int
	Instructions []*CompiledInnerInstruction
}

type TransactionMeta struct {
	Err               any
	Fee               uint64
	PostBalances      []uint64
	PreBalances       []uint64
	LogMessages       []string
	Version           any
	LoadedAddresses   *TransactionLoadedAddress
	PreTokenBalances  []*TransactionTokenBalance
	PostTokenBalances []*TransactionTokenBalance
	InnerInstructions []*TransactionInnerInstructions
}

type GetTransactionResponse struct {
	BlockTime   int64
	Meta        *TransactionMeta
	Slot        uint64
	Transaction [2]string
	Version     any
}

func (rpc *Rpc) GetMultipleTransactions(
	params []*GetMultipleTransactionsParams,
) ([]*Response[*Transaction], error) {
	requests := make([]*request, len(params))
	for i, p := range params {
		reqParam := map[string]any{
			"maxSupportedTransactionVersion": 0,
			"encoding":                       "base64",
			"commitment":                     p.Commitment,
		}
		if p.Commitment == "" {
			reqParam["commitment"] = CommitmentConfirmed
		}
		req := rpc.newRequest("getTransaction", []any{p.Signature, reqParam})
		requests[i] = req
	}

	responses := []*responseInternal[*GetTransactionResponse]{}
	err := rpc.sendRequest(requests, &responses)
	if err != nil {
		return nil, err
	}

	type r = *responseInternal[*GetTransactionResponse]
	slices.SortFunc(responses, func(a r, b r) int {
		return int(a.Id - b.Id)
	})

	parsedResponses := make([]*Response[*Transaction], len(responses))
	for i, res := range responses {
		if res.Error != nil {
			parsedResponses[i] = &Response[*Transaction]{
				Error: res.Error,
			}
		} else {
			compiledTx, err := parseTransaction(res.Result.Transaction[0])
			if err != nil {
				return nil, err
			}
			tx, err := decompileTransaction(
				res.Result.Slot,
				res.Result.BlockTime,
				res.Result.Meta,
				compiledTx,
			)
			if err != nil {
				return nil, err
			}
			parsedResponses[i] = &Response[*Transaction]{
				Result: tx,
			}
		}
	}

	return parsedResponses, nil
}

type GetMultipleBlocksSignaturesParams struct {
	Slot       uint64
	Commitment Commitment
}

type GetBlockSignaturesResponse struct {
	BlockHeight       uint64   `json:"blockHeight"`
	BlockTime         int64    `json:"blockTime"`
	Blockhash         string   `json:"blockhash"`
	ParentSlot        uint64   `json:"parentSlot"`
	PreviousBlockhash string   `json:"previousBlockhash"`
	Signatures        []string `json:"signatures"`
}

func (rpc *Rpc) GetMultipleBlocksSignatures(
	params []*GetMultipleBlocksSignaturesParams,
) ([]*Response[*GetBlockSignaturesResponse], error) {
	requests := make([]*request, len(params))
	for i, p := range params {
		reqParam := map[string]any{
			"maxSupportedTransactionVersion": 0,
			"encoding":                       "base64",
			"commitment":                     p.Commitment,
			"rewards":                        false,
			"transactionDetails":             "signatures",
		}
		if p.Commitment == "" {
			reqParam["commitment"] = CommitmentConfirmed
		}
		req := rpc.newRequest("getBlock", []any{p.Slot, reqParam})
		requests[i] = req
	}

	responses := []*responseInternal[*GetBlockSignaturesResponse]{}
	err := rpc.sendRequest(requests, &responses)
	if err != nil {
		return nil, err
	}

	type r = *responseInternal[*GetBlockSignaturesResponse]
	slices.SortFunc(responses, func(a r, b r) int {
		return int(a.Id - b.Id)
	})

	res := make([]*Response[*GetBlockSignaturesResponse], len(responses))
	for i, r := range responses {
		res[i] = &Response[*GetBlockSignaturesResponse]{
			Error:  r.Error,
			Result: r.Result,
		}
	}

	return res, nil
}

func (rpc *Rpc) GetBlockSignatures(
	slot uint64, commitment Commitment,
) (*Response[*GetBlockSignaturesResponse], error) {
	reqParams := map[string]any{
		"maxSupportedTransactionVersion": 0,
		"encoding":                       "base64",
		"commitment":                     commitment,
		"rewards":                        false,
		"transactionDetails":             "signatures",
	}
	resInteral := new(responseInternal[*GetBlockSignaturesResponse])
	req := rpc.newRequest("getBlock", []any{slot, reqParams})

	err := rpc.sendRequest(req, resInteral)
	if err != nil {
		return nil, err
	}

	res := &Response[*GetBlockSignaturesResponse]{
		Result: resInteral.Result,
		Error:  resInteral.Error,
	}

	return res, nil
}
