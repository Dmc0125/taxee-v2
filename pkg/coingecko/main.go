package coingecko

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"
	apiutils "taxee/pkg/api_utils"
	"taxee/pkg/assert"
	"taxee/pkg/logger"
	"time"

	"github.com/shopspring/decimal"
)

const (
	demoApiUrl = "https://api.coingecko.com/api/v3"
	proApiUrl  = "https://pro-api.coingecko.com/api/v3"
)

var apiKey, apiType string
var initDone atomic.Int32
var reqLimiter *apiutils.Limiter

func Init(nApiKey, nApiType string) {
	if initDone.Add(1) != 1 {
		assert.True(false, "init already done")
	}

	apiKey = nApiKey
	apiType = nApiType

	if apiType == "pro" {
		reqLimiter = apiutils.NewLimiter(50)
	} else {
		reqLimiter = apiutils.NewLimiter(1000)
	}
}

func newRequest(endpoint string) (*http.Request, error) {
	apiUrl, header := proApiUrl, "x-cg-pro-api-key"
	if apiType == "demo" {
		apiUrl, header = demoApiUrl, "x-cg-demo-api-key"
	}

	req, err := http.NewRequest(
		"GET", fmt.Sprintf("%s%s", apiUrl, endpoint), nil,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %w", err)
	}
	req.Header.Add(header, apiKey)

	return req, nil
}

type CoingeckoCoin struct {
	Id        string            `json:"id"`
	Symbol    string            `json:"symbol"`
	Name      string            `json:"name"`
	Platforms map[string]string `json:"platforms"`
}

func GetCoins(ctx context.Context) ([]*CoingeckoCoin, error) {
	req, err := newRequest("/coins/list?include_platform=true")
	if err != nil {
		return nil, err
	}

	if reqLimiter != nil {
		reqLimiter.Lock()
		defer reqLimiter.Free()
	}

	var res []*CoingeckoCoin
	statusCode, statusOk, err := apiutils.HttpSend(ctx, req, &res)
	if err != nil {
		return nil, err
	}
	if !statusOk {
		return nil, fmt.Errorf("status not ok: %d", statusCode)
	}
	return res, nil
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
	ctx context.Context,
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
	req, err := newRequest(endpoint)
	if err != nil {
		return nil, err
	}

	if reqLimiter != nil {
		reqLimiter.Lock()
		defer reqLimiter.Free()
	}

	statusCode, statusOk, err := apiutils.HttpSend(ctx, req, &res)
	if err != nil {
		return nil, err
	}
	if !statusOk {
		return nil, fmt.Errorf("status not ok: %d", statusCode)
	}
	return res.Prices, nil
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

type MissingPricepointsRange struct {
	TimestampFrom time.Time
	TimestampTo   time.Time
}

func getMissingPricepoints(ohlc []*OhlcCoinData, from, to time.Time) []*MissingPricepointsRange {
	missing := make([]*MissingPricepointsRange, 0)

	if len(ohlc) == 0 {
		missing = append(missing, &MissingPricepointsRange{
			TimestampFrom: from,
			TimestampTo:   to,
		})
	} else {
		for i := 0; i < len(ohlc); i += 1 {
			pp := ohlc[i]

			if !pp.Timestamp.Equal(from) {
				missing = append(missing, &MissingPricepointsRange{
					TimestampFrom: from,
					TimestampTo:   pp.Timestamp.Add(-time.Hour),
				})
			}

			from = pp.Timestamp.Add(time.Hour)
		}

		if from != to.Add(time.Hour) {
			missing = append(missing, &MissingPricepointsRange{
				TimestampFrom: from,
				TimestampTo:   to,
			})
		}
	}

	return missing
}

func GetCoinOhlc(
	ctx context.Context,
	coinId string,
	vsCurrency FiatCurrency,
	from time.Time,
) ([]*OhlcCoinData, []*MissingPricepointsRange, error) {
	fromUnix := from.Unix()
	toUnix := fromUnix + 60*60*24*31
	if now := time.Now().Unix(); toUnix > now {
		toUnix = now
	}

	if apiType == "demo" {
		logger.Warn("Ingoring get coin ohlc - demo api")

		res := make([]*OhlcCoinData, 0)
		missing := getMissingPricepoints(res, from, time.Unix(toUnix, 0))

		return res, missing, nil
	} else {
		endpoint := fmt.Sprintf(
			"/coins/%s/ohlc/range?vs_currency=%s&from=%d&to=%d&interval=hourly&precision=full",
			coinId,
			vsCurrency,
			fromUnix,
			toUnix,
		)
		req, err := newRequest(endpoint)
		if err != nil {
			return nil, nil, err
		}

		if reqLimiter != nil {
			reqLimiter.Lock()
			defer reqLimiter.Free()
		}

		res := make([]*OhlcCoinData, 0)
		statusCode, statusOk, err := apiutils.HttpSend(ctx, req, &res)
		if err != nil {
			return nil, nil, err
		}
		if !statusOk {
			return nil, nil, fmt.Errorf("status not ok: %d", statusCode)
		}

		missing := getMissingPricepoints(res, from, time.Unix(toUnix, 0))
		return res, missing, nil
	}
}

type CoinMetadata struct {
	Image struct {
		Thumb string `json:"thumb"`
		Small string `json:"small"`
		Large string `json:"large"`
	} `json:"image"`
}

func GetCoinMetadata(ctx context.Context, coinId string) (CoinMetadata, error) {
	endpoint := fmt.Sprintf("/coins/%s", coinId)

	req, err := newRequest(endpoint)
	if err != nil {
		return CoinMetadata{}, err
	}

	if reqLimiter != nil {
		reqLimiter.Lock()
		defer reqLimiter.Free()
	}

	var res CoinMetadata
	statusCode, statusOk, err := apiutils.HttpSend(ctx, req, &res)
	if err != nil {
		return res, err
	}
	if !statusOk {
		return res, fmt.Errorf("status not ok: %d", statusCode)
	}

	return res, nil
}
