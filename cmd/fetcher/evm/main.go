package evm

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"taxee/pkg/assert"
	"time"
)

const apiUrl = "https://api.etherscan.io/v2/api"

type Client struct {
	apiKey string

	etherscanTimeout   int64
	etherscanMx        *sync.Mutex
	etherscanLastReqTs time.Time
}

func NewClient() *Client {
	apiKey := os.Getenv("ETHERSCAN_API_KEY")
	assert.True(len(apiKey) > 0, "missing ETHERSCAN_API_KEY")

	return &Client{
		apiKey: apiKey,

		etherscanTimeout: 200,
		etherscanMx:      &sync.Mutex{},
	}
}

type etherscanResponse[T any] struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Result  T      `json:"result"`
}

func (client *Client) sendRequest(
	request *http.Request,
	data any,
	timeout int64,
) error {
	client.etherscanMx.Lock()
	if diff := (client.etherscanLastReqTs.UnixMilli() + timeout) - time.Now().UnixMilli(); diff > 0 {
		time.Sleep(time.Duration(diff * int64(time.Millisecond)))
	}

	res, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("unable to execute request: %w", err)
	}

	client.etherscanLastReqTs = time.Now()
	client.etherscanMx.Unlock()

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

type StringUint64 uint64

func (dst *StringUint64) UnmarshalJSON(src []byte) error {
	s := string(src[1 : len(src)-1])
	res, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fmt.Errorf("unable to parse string as uint64: %s %w", s, err)
	}
	*dst = StringUint64(res)
	return nil
}

type StringUint32 uint32

func (dst *StringUint32) UnmarshalJSON(src []byte) error {
	s := string(src[1 : len(src)-1])
	res, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return fmt.Errorf("unable to parse string as uint32: %s %w", s, err)
	}
	*dst = StringUint32(res)
	return nil
}

type StringTime time.Time

func (dst *StringTime) UnmarshalJSON(src []byte) error {
	s := string(src[1 : len(src)-1])
	res, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fmt.Errorf("unable to parse string as int64: %s %w", s, err)
	}
	*dst = StringTime(time.Unix(res, 0))
	return nil
}

type StringErr bool

func (dst *StringErr) UnmarshalJSON(src []byte) error {
	s := string(src[1 : len(src)-1])
	*dst = s != "0"
	return nil
}

type HexBytes []byte

func (dst *HexBytes) UnmarshalJSON(src []byte) error {
	s := string(src[1 : len(src)-1])
	if s == "0x" || len(s) == 0 {
		return nil
	}

	s = s[2:]
	if len(s)%2 == 1 {
		s = string(append(
			[]byte{'0'},
			s...,
		))
	}

	bytes, err := hex.DecodeString(s)
	if err != nil {
		return fmt.Errorf("invalid bytes: %s %w", s, err)
	}
	*dst = bytes
	return nil
}

type EtherscanTransaction struct {
	BlockNumber StringUint64 `json:"blockNumber"`
	Timestamp   StringTime   `json:"timeStamp"`
	Hash        string       `json:"hash"`
	TxIdx       StringUint32 `json:"transactionIndex"`
	From        string       `json:"from"`
	To          string       `json:"to"`
	Value       StringUint64 `json:"value"`
	Gas         StringUint64 `json:"gas"`
	GasPrice    StringUint64 `json:"gasPrice"`
	GasUsed     StringUint64 `json:"gasUsed"`
	Err         StringErr    `json:"isError"`
	Input       HexBytes     `json:"input"`
	Contract    string       `json:"contractAddress"`
}

func (client *Client) GetWalletNormalTransactions(
	chainId int,
	walletAddress string,
	startBlock, endBlock uint64,
) ([]*EtherscanTransaction, error) {
	url := fmt.Sprintf(
		"%s?chainId=%d&module=account&action=txlist&address=%s&startBlock=%d&endBlock=%d&sort=asc&apiKey=%s",
		apiUrl,
		chainId,
		walletAddress,
		startBlock,
		endBlock,
		client.apiKey,
	)
	req, err := http.NewRequest("GET", url, nil)
	assert.NoErr(err, "")

	var data etherscanResponse[[]*EtherscanTransaction]
	err = client.sendRequest(req, &data, client.etherscanTimeout)
	return data.Result, err
}

type TopicOperator string

const (
	TopicAnd TopicOperator = "and"
	TopicOr  TopicOperator = "or"
)

type HexUint64 uint64

func (dst *HexUint64) UnmarshalJSON(src []byte) error {
	s := string(src[1 : len(src)-1])
	if s == "0x" {
		return nil
	}

	res, err := strconv.ParseInt(s, 0, 64)
	if err != nil {
		return fmt.Errorf("unable to convert string to uint64: %s %w", s, err)
	}
	*dst = HexUint64(res)
	return nil
}

type HexUint32 uint32

func (dst *HexUint32) UnmarshalJSON(src []byte) error {
	s := string(src[1 : len(src)-1])
	if s == "0x" {
		return nil
	}

	res, err := strconv.ParseInt(s, 0, 32)
	if err != nil {
		return fmt.Errorf("unable to convert string to uint32: %s %w", s, err)
	}
	*dst = HexUint32(res)
	return nil
}

type HexTime time.Time

func (dst *HexTime) UnmarshalJSON(src []byte) error {
	s := string(src[1 : len(src)-1])
	if s == "0x" {
		return nil
	}

	res, err := strconv.ParseInt(s, 0, 64)
	if err != nil {
		return fmt.Errorf("unable to convert string to int64: %s %w", s, err)
	}
	*dst = HexTime(time.Unix(res, 0))
	return nil
}

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
		if topic != nil && len(topic) == 32 {
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
		apiUrl,
		chainId,
		startBlock,
		endBlock,
		client.apiKey,
		topicsQuery.String(),
	)
	req, err := http.NewRequest("GET", url, nil)
	assert.NoErr(err, "")

	var data etherscanResponse[[]*EtherscanEvent]
	err = client.sendRequest(req, &data, client.etherscanTimeout)
	return data.Result, err
}

type EtherscanInternalTransaction struct {
	BlockNumber     StringUint64 `json:"blockNumber"`
	Hash            string       `json:"hash"`
	From            string       `json:"from"`
	To              string       `json:"to"`
	Value           StringUint64 `json:"value"`
	ContractAddress string       `json:"contractAddress"`
	Input           HexBytes     `json:"input"`
	Timestamp       StringTime   `json:"timeStamp"`
}

func (client *Client) GetInternalTransactionsByAddress(
	chainId int,
	address string,
	startBlock, endBlock uint64,
) ([]*EtherscanInternalTransaction, error) {
	url := fmt.Sprintf(
		"%s?chainid=%d&module=account&action=txlistinternal&address=%s&startBlock=%d&endBlock=%d&apiKey=%s",
		apiUrl,
		chainId,
		address,
		startBlock,
		endBlock,
		client.apiKey,
	)
	req, err := http.NewRequest("GET", url, nil)
	assert.NoErr(err, "")

	var data etherscanResponse[[]*EtherscanInternalTransaction]
	err = client.sendRequest(req, &data, client.etherscanTimeout)
	return data.Result, err
}

type RpcResponse[T any] struct {
	Id     int `json:"id"`
	Result T   `json:"result"`
}

type RpcTransaction struct {
	Hash        string    `json:"hash"`
	BlockHash   string    `json:"blockHash"`
	BlockNumber HexUint64 `json:"blockNumber"`
	TxIdx       HexUint32 `json:"transactionIndex"`
	From        string    `json:"from"`
	To          string    `json:"to"`
	Value       HexUint64 `json:"value"`
	Input       HexBytes  `json:"input"`
	Gas         HexUint64 `json:"gas"`
	GasPrice    HexUint64 `json:"gasPrice"`
}

func (client *Client) GetTransactionByHash(
	chainId int,
	hash string,
) (*RpcTransaction, error) {
	url := fmt.Sprintf(
		"%s?chainid=%d&module=proxy&action=eth_getTransactionByHash&txhash=%s&apiKey=%s",
		apiUrl,
		chainId,
		hash,
		client.apiKey,
	)
	req, err := http.NewRequest("GET", url, nil)
	assert.NoErr(err, "")

	var data RpcResponse[*RpcTransaction]
	err = client.sendRequest(req, &data, client.etherscanTimeout)
	return data.Result, err
}

type ReceiptErr bool

func (dst *ReceiptErr) UnmarshalJSON(src []byte) error {
	s := string(src[1 : len(src)-1])
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
	GasUsed     HexUint64                 `json:"gasUsed"`
	Err         ReceiptErr                `json:"status"`
	Logs        []*RpcTransactionEventLog `json:"logs"`
}

func (client *Client) GetTransactionReceipt(
	chainId int,
	hash string,
) (*RpcTransactionReceipt, error) {
	url := fmt.Sprintf(
		"%s?chainid=%d&module=proxy&action=eth_getTransactionReceipt&txhash=%s&apiKey=%s",
		apiUrl,
		chainId,
		hash,
		client.apiKey,
	)
	req, err := http.NewRequest("GET", url, nil)
	assert.NoErr(err, "")

	var data RpcResponse[*RpcTransactionReceipt]
	err = client.sendRequest(req, &data, client.etherscanTimeout)
	return data.Result, err
}
