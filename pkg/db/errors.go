package db

import (
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

type ParserErrorType uint8

const (
	ParserErrorTypeMissingAccount ParserErrorType = iota
	ParserErrorTypeAccountBalanceMismatch
	ParserErrorTypeAccountDataMismatch
)

type ParserError struct {
	TxId     string
	IxIdx    int32
	EventIdx int32

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
