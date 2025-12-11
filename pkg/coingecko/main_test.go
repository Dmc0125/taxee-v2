package coingecko

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetMissingPricepoints(t *testing.T) {
	const day = 60 * 60
	start := time.Unix(time.Now().Unix()/day*day, 0)
	end := start.Add(4 * time.Hour)

	{
		ohlcMissingFirst := []*OhlcCoinData{
			// {Timestamp: start},
			// {Timestamp: start.Add(1 * time.Hour)},
			// {Timestamp: start.Add(2 * time.Hour)},
			// {Timestamp: start.Add(3 * time.Hour)},
			{Timestamp: start.Add(4 * time.Hour)},
		}
		missing := getMissingPricepoints(ohlcMissingFirst, start, end)

		assert.Len(t, missing, 1)
		m := missing[0]
		assert.Equal(t, m.TimestampFrom, start)
		assert.Equal(t, m.TimestampTo, start.Add(3*time.Hour))
	}

	{
		ohlcMissingMiddle := []*OhlcCoinData{
			{Timestamp: start},
			{Timestamp: start.Add(1 * time.Hour)},
			// {Timestamp: start.Add(2 * time.Hour)},
			// {Timestamp: start.Add(3 * time.Hour)},
			{Timestamp: start.Add(4 * time.Hour)},
		}
		missing := getMissingPricepoints(ohlcMissingMiddle, start, end)

		assert.Len(t, missing, 1)
		m := missing[0]
		assert.Equal(t, m.TimestampFrom, start.Add(2*time.Hour))
		assert.Equal(t, m.TimestampTo, start.Add(3*time.Hour))
	}

	{
		ohlcMissingLast := []*OhlcCoinData{
			{Timestamp: start},
			{Timestamp: start.Add(1 * time.Hour)},
			{Timestamp: start.Add(2 * time.Hour)},
			// {Timestamp: start.Add(3 * time.Hour)},
			// {Timestamp: start.Add(4 * time.Hour)},
		}
		missing := getMissingPricepoints(ohlcMissingLast, start, end)

		assert.Len(t, missing, 1)
		m := missing[0]
		assert.Equal(t, m.TimestampFrom, start.Add(3*time.Hour))
		assert.Equal(t, m.TimestampTo, start.Add(4*time.Hour))

	}
}
