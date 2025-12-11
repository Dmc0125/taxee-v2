package solana

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"sync/atomic"
	"taxee/pkg/assert"
)

type requestTimer interface {
	Lock()
	Free()
}

type ResponseError struct {
	Code    int
	Message string
	Data    any
}

type Response[T any] struct {
	Jsonrpc string
	Id      uint64
	Result  T
	Error   *ResponseError
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
	reqId atomic.Uint64
	url   string
	timer requestTimer
}

func NewRpc(timer requestTimer) *Rpc {
	url := os.Getenv("SOLANA_RPC_URL")
	assert.True(url != "", "missing SOLANA_RPC_URL")

	return &Rpc{
		url:   url,
		timer: timer,
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

var ERROR_STATUS_CODE_NOT_OK = errors.New("response status != 200")

func (rpc *Rpc) sendRequest(reqData any, resData any) error {
	reqBody, err := json.Marshal(reqData)
	assert.NoErr(err, "unable to marshal solana RPC request")

	rpc.timer.Lock()

	res, err := http.Post(rpc.url, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("unable to execute request: %s", err)
	}
	defer res.Body.Close()

	rpc.timer.Free()

	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("unable to read body: %s", err)
	}
	if res.StatusCode != 200 {
		return fmt.Errorf(
			"%w\nBody: %s", ERROR_STATUS_CODE_NOT_OK, string(resBody),
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

	res := new(Response[[]*GetSignaturesForAddressResponse])
	err := rpc.sendRequest(req, res)
	return res, err
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
) ([]*Response[*GetTransactionResponse], error) {
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

	responses := []*Response[*GetTransactionResponse]{}
	err := rpc.sendRequest(requests, &responses)

	type r = *Response[*GetTransactionResponse]
	slices.SortFunc(responses, func(a r, b r) int {
		return int(a.Id - b.Id)
	})

	return responses, err
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

	responses := []*Response[*GetBlockSignaturesResponse]{}
	err := rpc.sendRequest(requests, &responses)
	if err != nil {
		return nil, err
	}

	type r = *Response[*GetBlockSignaturesResponse]
	slices.SortFunc(responses, func(a r, b r) int {
		return int(a.Id - b.Id)
	})

	return responses, nil
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
	res := new(Response[*GetBlockSignaturesResponse])
	req := rpc.newRequest("getBlock", []any{slot, reqParams})
	err := rpc.sendRequest(req, res)
	return res, err
}
