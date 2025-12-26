package evm

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"
	"taxee/pkg/assert"
	"taxee/pkg/db"
	"taxee/pkg/jsonrpc"
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

const (
	GetTransactionByHash  = "eth_getTransactionByHash"
	GetTransactionReceipt = "eth_getTransactionReceipt"
	GetCode               = "eth_getCode"
	GetStorageAt          = "eth_getStorageAt"
	Call                  = "eth_call"
)

var AlchemyMethodsCosts = map[string]int{
	GetTransactionByHash:  20,
	GetTransactionReceipt: 20,
	GetCode:               20,
	GetStorageAt:          20,
	Call:                  26,
}

func AlchemyApiUrl(network db.Network, apiKey string) (string, error) {
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
		return "", fmt.Errorf("unsupported EVM network: %d", network)
	}

	url := fmt.Sprintf(
		"https://%s.g.alchemy.com/v2/%s",
		alchemyNetwork, apiKey,
	)

	return url, nil
}

func unmarshalUnformattedData(dst *[]byte, src []byte, expectedLen int, name string) error {
	if len(src) < 2 {
		return fmt.Errorf("unable to unmarshal %s: invalid: %s", name, string(src))
	}
	src = src[1 : len(src)-1] // remove json quotes

	if string(src[:2]) != "0x" {
		return fmt.Errorf("unable to unmarshal %s: missing prefix: %s", name, string(src))
	}
	src = src[2:] // remove 0x prefix

	if expectedLen != 0 && len(src) != expectedLen*2 {
		return fmt.Errorf("unable to unmarshal %s: invalid len: %s", name, string(src))
	}

	if len(src) == 0 {
		return nil
	}

	bytes, err := hex.DecodeString(string(src))
	if err != nil {
		return fmt.Errorf("unable to unmarshal %s: invalid hex: %w: %s", name, err, string(src))
	}

	if len(*dst) == 0 {
		*dst = append(*dst, bytes...)
	} else {
		copy(*dst, bytes)
	}

	return nil
}

func unmarshalQuantityUnsigned(src []byte, size int, name string) (uint64, error) {
	if len(src) < 2 {
		return 0, fmt.Errorf("unable to unmarshal %s: invalid: %s", name, string(src))
	}
	src = src[1 : len(src)-1] // remove json quotes
	return strconv.ParseUint(string(src), 0, size)
}

type DataBytes32 [32]byte

var _ json.Unmarshaler = (*DataBytes32)(nil)

func (b *DataBytes32) UnmarshalJSON(src []byte) error {
	t := (*b)[:]
	return unmarshalUnformattedData(&t, src, 32, "bytes32")
}

type DataBytes []byte

func (b DataBytes) MarshalJSON() ([]byte, error) {
	encoded := "0x" + hex.EncodeToString(b)
	return fmt.Appendf(nil, `"%s"`, encoded), nil
}

func (b *DataBytes) UnmarshalJSON(src []byte) error {
	return unmarshalUnformattedData((*[]byte)(b), src, 0, "bytes")
}

var _ json.Unmarshaler = (*DataBytes)(nil)
var _ json.Marshaler = (*DataBytes)(nil)

type QuantityUint64 uint64

func (q *QuantityUint64) UnmarshalJSON(src []byte) error {
	_q, err := unmarshalQuantityUnsigned(src, 64, "quantityUint64")
	if err == nil {
		*q = QuantityUint64(_q)
	}
	return err
}

func (q QuantityUint64) MarshalJSON() ([]byte, error) {
	return fmt.Appendf(nil, `"0x%x"`, q), nil
}

var _ json.Unmarshaler = (*QuantityUint64)(nil)
var _ json.Marshaler = (*QuantityUint64)(nil)

type QuantityBigInt big.Int

func (q *QuantityBigInt) UnmarshalJSON(src []byte) error {
	if len(src) < 2 {
		return fmt.Errorf("unable to unmarshal quantityBigInt: invalid: %s", string(src))
	}
	src = src[1 : len(src)-1] // remove json quotes

	b, ok := new(big.Int).SetString(string(src), 0)
	if !ok {
		return fmt.Errorf("unable to unmarshal quantityBigInt: invalid hex: %s", string(src))
	}

	*q = QuantityBigInt(*b)
	return nil
}

var _ json.Unmarshaler = (*QuantityBigInt)(nil)

type Hash string

func (a *Hash) UnmarshalJSON(src []byte) error {
	if len(src) < 2 {
		return fmt.Errorf("unable to unmarshal address: invalid: %s", string(src))
	}
	src = src[1 : len(src)-1] // remove json quotes

	*a = Hash(src)
	return nil
}

var _ json.Unmarshaler = (*Hash)(nil)

type GetTransactionParams string

func (g GetTransactionParams) MarshalJSON() ([]byte, error) {
	return json.Marshal([]string{string(g)})
}

var _ json.Marshaler = (*GetTransactionParams)(nil)

type GetTransactionByHashResult struct {
	BlockHash        Hash           `json:"blockHash"`
	BlockNumber      QuantityUint64 `json:"blockNumber"`
	From             Hash           `json:"from"`
	Gas              QuantityBigInt `json:"gas"`
	GasPrice         QuantityBigInt `json:"gasPrice"`
	Hash             Hash           `json:"hash"`
	Input            DataBytes      `json:"input"`
	Nonce            QuantityUint64 `json:"nonce"`
	To               Hash           `json:"to"`
	TransactionIndex QuantityUint64 `json:"transactionIndex"`
	Value            QuantityBigInt `json:"value"`
}

func (g *GetTransactionByHashResult) Validate() error {
	if len(g.BlockHash) != 66 {
		return fmt.Errorf("invalid blockhash")
	}
	if g.BlockNumber == 0 {
		return fmt.Errorf("missing quantity")
	}
	if len(g.From) != 42 {
		return fmt.Errorf("invalid from address")
	}
	if len(g.To) != 42 {
		return fmt.Errorf("invalid to address")
	}
	if len(g.Hash) != 66 {
		return fmt.Errorf("invalid hash")
	}
	return nil
}

var _ jsonrpc.Validator = (*GetTransactionByHashResult)(nil)

type Log struct {
	Address Hash           `json:"address"`
	Topics  [4]DataBytes32 `json:"topics"`
	Data    DataBytes      `json:"data"`
}

func (l *Log) Validate() error {
	if len(l.Address) != 42 {
		return fmt.Errorf("invalid from address")
	}
	return nil
}

var _ jsonrpc.Validator = (*Log)(nil)

type GetTransactionReceiptResult struct {
	TransactionHash   Hash           `json:"transactionHash"`
	TransactionIndex  QuantityUint64 `json:"transactionIndex"`
	BlockHash         Hash           `json:"blockHash"`
	BlockNumber       QuantityUint64 `json:"blockNumber"`
	From              Hash           `json:"from"`
	To                Hash           `json:"to"`
	CumulativeGasUsed QuantityBigInt `json:"cumulativeGasUsed"`
	GasUsed           QuantityBigInt `json:"gasUsed"`
	ContractAddress   Hash           `json:"contractAddress"`
	Logs              []*Log         `json:"logs"`
	Type              QuantityUint64 `json:"type"`
	// 1 => success, 0 => error
	Status QuantityUint64 `json:"status"`
}

func (g *GetTransactionReceiptResult) Validate() error {
	if len(g.BlockHash) != 66 {
		return fmt.Errorf("invalid blockhash")
	}
	if g.BlockNumber == 0 {
		return fmt.Errorf("missing quantity")
	}
	if len(g.From) != 42 {
		return fmt.Errorf("invalid from address")
	}
	if len(g.To) != 42 {
		return fmt.Errorf("invalid to address")
	}
	if len(g.TransactionHash) != 66 {
		return fmt.Errorf("invalid hash")
	}

	for _, l := range g.Logs {
		if err := l.Validate(); err != nil {
			return err
		}
	}

	if g.Status > 1 {
		return fmt.Errorf("invalid status")
	}

	return nil
}

var _ jsonrpc.Validator = (*GetTransactionReceiptResult)(nil)

const (
	BlockTagLatest = "latest"
)

type GetCodeParams struct {
	Address     string `json:"address"`
	BlockNumber QuantityUint64
	BlockTag    string
}

func (g GetCodeParams) MarshalJSON() ([]byte, error) {
	p := []any{g.Address}
	if g.BlockTag != "" {
		p = append(p, g.BlockTag)
	} else {
		p = append(p, g.BlockNumber)
	}
	return json.Marshal(p)
}

var _ json.Marshaler = (*GetCodeParams)(nil)

type GetCodeResult DataBytes

type CallParams struct {
	To          string         `json:"to"`
	Input       DataBytes      `json:"input"`
	BlockNumber QuantityUint64 `json:"-"`
	BlockTag    string         `json:"-"`
}

func (c CallParams) MarshalJSON() ([]byte, error) {
	o := struct {
		To    string    `json:"to"`
		Input DataBytes `json:"input"`
	}{c.To, c.Input}
	p := []any{o}
	if c.BlockTag != "" {
		p = append(p, c.BlockTag)
	} else {
		p = append(p, c.BlockNumber)
	}

	return json.Marshal(p)
}

var _ json.Marshaler = (*CallParams)(nil)

type GetStorageAtParams struct {
	Address     string
	StorageSlot DataBytes
	BlockNumber QuantityUint64
	BlockTag    string
}

func (g GetStorageAtParams) MarshalJSON() ([]byte, error) {
	p := []any{g.Address, g.StorageSlot}
	if g.BlockTag != "" {
		p = append(p, g.BlockTag)
	} else {
		p = append(p, g.BlockNumber)
	}
	return json.Marshal(p)
}

var _ json.Marshaler = (*GetStorageAtParams)(nil)
