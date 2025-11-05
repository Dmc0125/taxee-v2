package db

import (
	"time"

	"github.com/shopspring/decimal"
)

type ErrOrigin uint8

const (
	ErrOriginPreprocess ErrOrigin = iota
	ErrOriginProcess
)

type ParserErrorMissingAccount struct {
	AccountAddress string `json:"accountAddress"`
}

// NOTE: balance validation after preprocess / process
type ParserErrorAccountBalanceMismatch struct {
	AccountAddress string          `json:"accountAddress"`
	Token          string          `json:"token"`
	Expected       decimal.Decimal `json:"expected"`
	Real           decimal.Decimal `json:"real"`
}

type ParserErrorAccountDataMismatch struct {
	AccountAddress string `json:"accountAddress"`
	Message        string `json:"message"`
}

type ParserErrorMissingPrice struct {
	Token     string    `json:"token"`
	Timestamp time.Time `json:"-"`
}

type ParserErrorType uint8

const (
	ParserErrorTypeMissingAccount ParserErrorType = iota
	ParserErrorTypeAccountBalanceMismatch
	ParserErrorTypeAccountDataMismatch
	ParserErrorTypeMissingPrice
)

// func NewParserErrorTypeFromString(s string) (ParserErrorType, error) {
// 	switch s {
// 	case "missing_account":
// 		return ParserErrorTypeMissingAccount, nil
// 	case "account_balance_mismatch":
// 		return ParserErrorTypeAccountBalanceMismatch, nil
// 	case "missing_price":
// 		return ParserErrorTypeMissingPrice, nil
// 	default:
// 		return 0, fmt.Errorf("invalid parser error type: %s", s)
// 	}
// }
//
// func (dst *ParserErrorType) Scan(src any) error {
// 	if src == nil {
// 		return nil
// 	}
//
// 	t, ok := src.(string)
// 	if !ok {
// 		return fmt.Errorf("invalid parser error type: %T", src)
// 	}
//
// 	var err error
// 	*dst, err = NewParserErrorTypeFromString(t)
//
// 	return err
// }
//
// func (e ParserErrorType) Value() (driver.Value, error) {
// 	switch e {
// 	case ParserErrorTypeMissingAccount:
// 		return "missing_account", nil
// 	case ParserErrorTypeAccountBalanceMismatch:
// 		return "account_balance_mismatch", nil
// 	case ParserErrorTypeMissingPrice:
// 		return "missing_price", nil
// 	case ParserErrorTypeInsufficientBalance:
// 		return "insufficient_balance", nil
// 	default:
// 		assert.True(false, "invalid parser error type: %d", e)
// 		return nil, nil
// 	}
// }

type ParserError struct {
	TxId  string
	IxIdx uint32

	Type ParserErrorType
	Data any
}

// InsertParserError
//
//	insert into
//		parser_err (
//			user_account_id, tx_id, ix_idx, origin, type, data
//		)
//	values (
//		$1, $2, $3, $4, $5, $6
//	)
const InsertParserError string = `
	insert into
		parser_err (
			user_account_id, tx_id, ix_idx, origin, type, data
		)
	values (
		$1, $2, $3, $4, $5, $6
	)
`
