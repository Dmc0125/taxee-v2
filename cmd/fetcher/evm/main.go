package evm

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"taxee/pkg/assert"
	"taxee/pkg/db"
	requesttimer "taxee/pkg/request_timer"
)

type requestTimer interface {
	Lock()
	Free()
}

const etherscanApiUrl = "https://api.etherscan.io/v2/api"

type Client struct {
	etherscanApiKey string
	etherscanTimer  *requesttimer.DefaultTimer

	alchemyApiKey string
	alchemyTimer  requestTimer
}

func NewClient(alchemyTimer requestTimer) *Client {
	etherscanApiKey := os.Getenv("ETHERSCAN_API_KEY")
	assert.True(len(etherscanApiKey) > 0, "missing ETHERSCAN_API_KEY")

	alchemyApiKey := os.Getenv("ALCHEMY_API_KEY")
	assert.True(len(alchemyApiKey) > 0, "missing ALCHEMY_API_KEY")

	return &Client{
		etherscanApiKey: etherscanApiKey,
		etherscanTimer:  requesttimer.NewDefault(200),

		alchemyApiKey: alchemyApiKey,
		alchemyTimer:  alchemyTimer,
	}
}

func ChainIdAndNativeDecimals(network db.Network) (chainId, decimals int, err error) {
	switch network {
	case db.NetworkArbitrum:
		chainId = 42161
	case db.NetworkEthereum:
		chainId = 1
	case db.NetworkAvaxC:
		chainId = 43114
	case db.NetworkBsc:
		chainId = 56
	default:
		err = fmt.Errorf("invalid EVM network: %d", network)
		return
	}

	switch network {
	case db.NetworkArbitrum, db.NetworkEthereum, db.NetworkBsc, db.NetworkAvaxC:
		decimals = 18
	default:
		err = fmt.Errorf("invalid EVM network: %d", network)
		return
	}

	return
}

func (client *Client) sendRequest(
	request *http.Request,
	data any,
	timer requestTimer,
) error {
	timer.Lock()

	res, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("unable to execute request: %w", err)
	}

	timer.Free()
	defer res.Body.Close()

	dataBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("unable to read body: %w", err)
	}

	if err := json.Unmarshal(dataBytes, data); err != nil {
		return fmt.Errorf("unable to unmarshal response: %w\nbody: %s", err, string(dataBytes))
	}

	return nil
}

////////////////
// ETHERSCAN methods

type etherscanResponse[T any] struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Result  T      `json:"result"`
}

type EtherscanTransaction struct {
	BlockNumber StringUint64  `json:"blockNumber"`
	Timestamp   StringTime    `json:"timeStamp"`
	Hash        string        `json:"hash"`
	TxIdx       StringUint32  `json:"transactionIndex"`
	From        string        `json:"from"`
	To          string        `json:"to"`
	Value       *StringBigInt `json:"value"`
	Gas         *StringBigInt `json:"gas"`
	GasPrice    *StringBigInt `json:"gasPrice"`
	GasUsed     *StringBigInt `json:"gasUsed"`
	Err         StringErr     `json:"isError"`
	Input       HexBytes      `json:"input"`
	Contract    string        `json:"contractAddress"`
}

func (client *Client) GetWalletNormalTransactions(
	chainId int,
	walletAddress string,
	startBlock, endBlock uint64,
) ([]*EtherscanTransaction, error) {
	url := fmt.Sprintf(
		"%s?chainId=%d&module=account&action=txlist&address=%s&startBlock=%d&endBlock=%d&sort=asc&apiKey=%s",
		etherscanApiUrl,
		chainId,
		walletAddress,
		startBlock,
		endBlock,
		client.etherscanApiKey,
	)
	req, err := http.NewRequest("GET", url, nil)
	assert.NoErr(err, "")

	var data etherscanResponse[[]*EtherscanTransaction]
	err = client.sendRequest(req, &data, client.etherscanTimer)
	return data.Result, err
}

type TopicOperator string

const (
	TopicAnd TopicOperator = "and"
	TopicOr  TopicOperator = "or"
)

type EtherscanEvent struct {
	Address     string      `json:"address"`
	Topics      [4]HexBytes `json:"topics"`
	Data        HexBytes    `json:"data"`
	BlockNumber HexUint64   `json:"blockNumber"`
	Timestamp   HexTime     `json:"timeStamp"`
	LogIdx      HexUint32   `json:"logIndex"`
	Hash        string      `json:"transactionHash"`
}

// Topic operators
//
// idx | topic numbers
//
// 0   | 0,1
//
// 1   | 1,2
//
// 2   | 2,3
//
// 3   | 0,2
//
// 4   | 0,3
//
// 5   | 1,3
func (client *Client) GetEventLogsByTopics(
	chainId int,
	startBlock, endBlock uint64,
	topics [4][]byte,
	topicsOperators [6]TopicOperator,
) ([]*EtherscanEvent, error) {
	topicsQueryParams := [4]string{
		"topic0", "topic1", "topic2", "topic3",
	}
	topicsOperatorsQueryParams := [6]string{
		"topic0_1_opr", "topic1_2_opr", "topic2_3_opr",
		"topic0_2_opr", "topic1_3_opr", "topic1_3_opr",
	}
	topicsQuery := strings.Builder{}

	for i, topic := range topics {
		if len(topic) == 32 {
			topicsQuery.WriteRune('&')
			topicsQuery.WriteString(topicsQueryParams[i])
			topicsQuery.WriteRune('=')

			encoded := hex.EncodeToString(topic)
			topicsQuery.WriteString("0x")
			topicsQuery.WriteString(encoded)
		}
	}
	for i, opr := range topicsOperators {
		if opr != "" {
			topicsQuery.WriteRune('&')
			topicsQuery.WriteString(topicsOperatorsQueryParams[i])
			topicsQuery.WriteRune('=')
			topicsQuery.WriteString(string(opr))
		}
	}

	url := fmt.Sprintf(
		"%s?chainid=%d&module=logs&action=getLogs&fromBlock=%d&toBlock=%d&apiKey=%s%s",
		etherscanApiUrl,
		chainId,
		startBlock,
		endBlock,
		client.etherscanApiKey,
		topicsQuery.String(),
	)
	req, err := http.NewRequest("GET", url, nil)
	assert.NoErr(err, "")

	var data etherscanResponse[[]*EtherscanEvent]
	err = client.sendRequest(req, &data, client.etherscanTimer)
	return data.Result, err
}

type EtherscanInternalTxByAddress struct {
	BlockNumber     StringUint64  `json:"blockNumber"`
	Hash            string        `json:"hash"`
	From            string        `json:"from"`
	To              string        `json:"to"`
	Value           *StringBigInt `json:"value"`
	ContractAddress string        `json:"contractAddress"`
	Input           HexBytes      `json:"input"`
	Timestamp       StringTime    `json:"timeStamp"`
}

func (client *Client) GetInternalTransactionsByAddress(
	chainId int,
	address string,
	startBlock, endBlock uint64,
) ([]*EtherscanInternalTxByAddress, error) {
	url := fmt.Sprintf(
		"%s?chainid=%d&module=account&action=txlistinternal&address=%s&startBlock=%d&endBlock=%d&apiKey=%s",
		etherscanApiUrl,
		chainId,
		address,
		startBlock,
		endBlock,
		client.etherscanApiKey,
	)
	req, err := http.NewRequest("GET", url, nil)
	assert.NoErr(err, "")

	var data etherscanResponse[[]*EtherscanInternalTxByAddress]
	err = client.sendRequest(req, &data, client.etherscanTimer)
	return data.Result, err
}

type EtherscanInternalTxByHash struct {
	BlockNumber     StringUint64  `json:"blockNumber"`
	Timestamp       StringTime    `json:"timeStamp"`
	From            string        `json:"from"`
	To              string        `json:"to"`
	Value           *StringBigInt `json:"value"`
	ContractAddress string        `json:"contractAddress"`
	Input           HexBytes      `json:"input"`
}

func (client *Client) GetInternalTransactionsByHash(
	chainId int,
	hash string,
) ([]*EtherscanInternalTxByHash, error) {
	url := fmt.Sprintf(
		"%s?chainid=%d&module=account&action=txlistinternal&txhash=%s&apikey=%s",
		etherscanApiUrl,
		chainId,
		hash,
		client.etherscanApiKey,
	)
	req, err := http.NewRequest("GET", url, nil)
	assert.NoErr(err, "")

	var data etherscanResponse[[]*EtherscanInternalTxByHash]
	err = client.sendRequest(req, &data, client.etherscanTimer)
	return data.Result, err
}

///////////////////
// RPC methods

type RpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data"`
}

type RpcResponse[T any] struct {
	Id     int       `json:"id"`
	Result T         `json:"result"`
	Error  *RpcError `json:"error"`
}

func (client *Client) newAlchemyUrl(network db.Network) (string, error) {
	var alchemyNetwork string
	switch network {
	case db.NetworkArbitrum:
		alchemyNetwork = "arb-mainnet"
	case db.NetworkAvaxC:
		alchemyNetwork = "avax-mainnet"
	case db.NetworkBsc:
		alchemyNetwork = "bnb-mainnet"
	case db.NetworkEthereum:
		alchemyNetwork = "eth-mainnet"
	default:
		return "", fmt.Errorf("invalid EVM network: %d", network)
	}

	url := fmt.Sprintf(
		"https://%s.g.alchemy.com/v2/%s",
		alchemyNetwork,
		client.alchemyApiKey,
	)

	return url, nil
}

type RpcRequest struct {
	Jsonrpc string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
	Id      int    `json:"id"`
}

func (client *Client) NewRpcRequest(
	method string,
	params any,
	id int,
) *RpcRequest {
	r := RpcRequest{
		Jsonrpc: "2.0",
		Method:  method,
		Params:  params,
		Id:      id,
	}
	return &r
}

type RpcTransaction struct {
	Hash        string     `json:"hash"`
	BlockHash   string     `json:"blockHash"`
	BlockNumber HexUint64  `json:"blockNumber"`
	TxIdx       HexUint32  `json:"transactionIndex"`
	From        string     `json:"from"`
	To          string     `json:"to"`
	Value       *HexBigInt `json:"value"`
	Input       HexBytes   `json:"input"`
	Gas         *HexBigInt `json:"gas"`
	GasPrice    *HexBigInt `json:"gasPrice"`
}

func (client *Client) BatchGetTransactionByHash(
	network db.Network,
	hashes []string,
) ([]*RpcResponse[*RpcTransaction], error) {
	requests := make([]*RpcRequest, len(hashes))
	for i, hash := range hashes {
		requests[i] = client.NewRpcRequest(
			"eth_getTransactionByHash",
			[]string{hash},
			i,
		)
	}

	body, err := json.Marshal(requests)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal requests: %w", err)
	}

	url, err := client.newAlchemyUrl(network)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return nil, fmt.Errorf("unable to create http request: %w", err)
	}

	var data []*RpcResponse[*RpcTransaction]
	err = client.sendRequest(req, &data, client.alchemyTimer)
	return data, err
}

type ReceiptErr bool

func (dst *ReceiptErr) UnmarshalJSON(src []byte) error {
	s := string(src[3 : len(src)-1])
	// 0 => err
	// 1 => ok
	*dst = s == "0"
	return nil
}

type RpcTransactionEventLog struct {
	Address string      `json:"address"`
	Topics  [4]HexBytes `json:"topics"`
	Data    HexBytes    `json:"data"`
}

type RpcTransactionReceipt struct {
	Hash        string                    `json:"transactionHash"`
	BlockHash   string                    `json:"blockHash"`
	BlockNumber HexUint64                 `json:"blockNumber"`
	GasUsed     *HexBigInt                `json:"gasUsed"`
	Err         ReceiptErr                `json:"status"`
	Logs        []*RpcTransactionEventLog `json:"logs"`
}

func (client *Client) BatchGetTransactionReceipt(
	network db.Network,
	hashes []string,
) ([]*RpcResponse[*RpcTransactionReceipt], error) {
	requests := make([]*RpcRequest, len(hashes))
	for i, hash := range hashes {
		requests[i] = client.NewRpcRequest(
			"eth_getTransactionReceipt",
			[]string{hash},
			i,
		)
	}

	body, err := json.Marshal(requests)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal requests: %w", err)
	}

	url, err := client.newAlchemyUrl(network)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return nil, fmt.Errorf("unable to create http request: %w", err)
	}

	var data []*RpcResponse[*RpcTransactionReceipt]
	err = client.sendRequest(req, &data, client.alchemyTimer)
	return data, err

}

func (client *Client) GetCode(network db.Network, address string) (string, error) {
	rpcReq := client.NewRpcRequest(
		"eth_getCode",
		[]string{address, "latest"},
		1,
	)
	body, err := json.Marshal(rpcReq)
	assert.NoErr(err, "")

	url, err := client.newAlchemyUrl(network)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequest(
		"POST",
		url,
		bytes.NewBuffer(body),
	)
	if err != nil {
		return "", fmt.Errorf("unable to create http request: %w", err)
	}
	var data RpcResponse[string]
	err = client.sendRequest(req, &data, client.alchemyTimer)
	return data.Result, err
}

type BatchResult[T any] struct {
	Data  T
	Error *RpcError
}

func (client *Client) Batch(
	network db.Network,
	requests []*RpcRequest,
) ([]*BatchResult[any], error) {
	body, err := json.Marshal(requests)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal requests: %w", err)
	}

	url, err := client.newAlchemyUrl(network)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(
		"POST", url,
		bytes.NewBuffer(body),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to create http request: %w", err)
	}

	var data []RpcResponse[any]
	err = client.sendRequest(req, &data, client.alchemyTimer)
	if err != nil {
		return nil, err
	}

	res := make([]*BatchResult[any], len(data))
	for i, d := range data {
		if d.Error != nil {
			res[i] = &BatchResult[any]{
				Error: d.Error,
			}
		} else {
			res[i] = &BatchResult[any]{
				Data: d.Result,
			}
		}
	}

	return res, nil

}
