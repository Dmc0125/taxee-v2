package coingecko

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"taxee/pkg/assert"
	"time"

	"github.com/shopspring/decimal"
)

const (
	demoApiUrl = "https://api.coingecko.com/api/v3"
	proApiUrl  = "https://pro-api.coingecko.com/api/v3"
)

var apiKey, apiType string
var reqTimeoutMillis, lastReqUnixMillis int64
var lock sync.Mutex

func Init() {
	apiKey = os.Getenv("COINGECKO_API_KEY")
	assert.True(apiKey != "", "missing COINGECKO_API_KEY")
	apiType = os.Getenv("COINGECKO_API_TYPE")

	if apiType == "demo" {
		reqTimeoutMillis = 1000
	} else {
		reqTimeoutMillis = 50
	}
}

func sendRequest[T any](
	endpoint string,
	resBodyData *T,
) error {
	lock.Lock()
	nextReqUnixMillis := lastReqUnixMillis + reqTimeoutMillis
	if now := time.Now().UnixMilli(); now < nextReqUnixMillis {
		rem := nextReqUnixMillis - now
		time.Sleep(time.Duration(rem) * time.Millisecond)
	}

	assert.True(apiKey != "", "missing COINGECKO_API_KEY")
	apiUrl, header := proApiUrl, "x-cg-pro-api-key"
	if apiType == "demo" {
		apiUrl, header = demoApiUrl, "x-cg-demo-api-key"
	}

	req, err := http.NewRequest(
		"GET",
		fmt.Sprintf("%s%s", apiUrl, endpoint),
		nil,
	)
	assert.NoErr(err, "unable to create new request")
	req.Header.Add(header, apiKey)

	res, err := http.DefaultClient.Do(req)

	lastReqUnixMillis = time.Now().UnixMilli()
	lock.Unlock()

	if err != nil {
		return fmt.Errorf("unable to send request: %w", err)
	}

	defer res.Body.Close()
	resBody, err := io.ReadAll(res.Body)

	if err != nil {
		return fmt.Errorf("unable to read response body: %w", err)
	}
	if res.StatusCode != 200 {
		return fmt.Errorf("body: %s\nresponse status != 200", string(resBody))
	}
	if err = json.Unmarshal(resBody, resBodyData); err != nil {
		return fmt.Errorf(
			"body: %s\nunable to unmarshal response body: %w",
			string(resBody),
			err,
		)
	}

	return nil
}

type CoingeckoCoin struct {
	Id        string            `json:"id"`
	Symbol    string            `json:"symbol"`
	Name      string            `json:"name"`
	Platforms map[string]string `json:"platforms"`
}

func GetCoins() (res []*CoingeckoCoin, err error) {
	err = sendRequest("/coins/list?include_platform=true", &res)
	return
}

type FiatCurrency string

const (
	FiatCurrencyEur FiatCurrency = "eur"
)

type CoinPrice struct {
	Timestamp time.Time
	Price     decimal.Decimal
}

func (dest *CoinPrice) UnmarshalJSON(src []byte) error {
	// expects: [uint64,float]
	comma := 0
	for ; src[comma] != ','; comma += 1 {
	}

	var err error
	timeMillis, err := strconv.ParseInt(string(src[1:comma]), 10, 64)
	if err != nil {
		return fmt.Errorf("unable to unmarshal timestamp: %w", err)
	}
	dest.Timestamp = time.UnixMilli(timeMillis)

	dest.Price, err = decimal.NewFromString(string(src[comma : len(src)-1]))
	if err != nil {
		return fmt.Errorf("unable to unmarshal price: %w", err)
	}

	return nil
}

type marketChartResponse struct {
	Prices []*CoinPrice `json:"prices"`
}

func GetCoinPrices(
	coinId string,
	vsCurrency FiatCurrency,
	from time.Time,
) ([]*CoinPrice, error) {
	const ninetyDays = 60 * 60 * 24 * 90
	fromUnix := from.Unix()
	toUnix := fromUnix + ninetyDays
	endpoint := fmt.Sprintf(
		"/coins/%s/market_chart/range?vs_currency=%s&from=%d&to=%d",
		coinId,
		vsCurrency,
		fromUnix,
		toUnix,
	)

	var res marketChartResponse
	err := sendRequest(endpoint, &res)

	return res.Prices, err
}

type OhlcCoinData struct {
	Timestamp time.Time
	Open      decimal.Decimal
	Close     decimal.Decimal
}

func (dst *OhlcCoinData) UnmarshalJSON(src []byte) error {
	commasIdx := 0
	commas := [4]int{}
	for i := 0; commasIdx < 4; i += 1 {
		c := src[i]
		if c == ',' {
			commas[commasIdx] = i
			commasIdx += 1
		}
	}

	timestampMillis, err := strconv.ParseInt(string(src[1:commas[0]]), 10, 64)
	if err != nil {
		return fmt.Errorf("unable to parse timestamp: %w", err)
	}
	dst.Timestamp = time.UnixMilli(timestampMillis)

	openPrice, err := decimal.NewFromString(string(src[commas[0]+1 : commas[1]]))
	if err != nil {
		return fmt.Errorf("unable to parse open price: %w", err)
	}
	dst.Open = openPrice

	closePrice, err := decimal.NewFromString(string(src[commas[3]+1 : len(src)-1]))
	if err != nil {
		return fmt.Errorf("unable to parse close price: %w", err)
	}
	dst.Close = closePrice

	return nil
}

func GetCoinOhlc(
	coinId string,
	vsCurrency FiatCurrency,
	from time.Time,
) ([]*OhlcCoinData, error) {
	fromUnix := from.Unix()
	toUnix := fromUnix + 60*60*24*31
	if now := time.Now().Unix(); toUnix > now {
		toUnix = now
	}

	endpoint := fmt.Sprintf(
		"/coins/%s/ohlc/range?vs_currency=%s&from=%d&to=%d&interval=hourly&precision=full",
		coinId,
		vsCurrency,
		fromUnix,
		toUnix,
	)
	res := make([]*OhlcCoinData, 0)
	err := sendRequest(endpoint, &res)

	return res, err
}

type CoinMetadata struct {
	Image struct {
		Thumb string `json:"thumb"`
		Small string `json:"small"`
		Large string `json:"large"`
	} `json:"image"`
}

func GetCoinMetadata(coinId string) (res CoinMetadata, err error) {
	endpoint := fmt.Sprintf("/coins/%s", coinId)
	err = sendRequest(endpoint, &res)
	return
}
