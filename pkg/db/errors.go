package db

import (
	"github.com/shopspring/decimal"
)

// Errors origins (where the error can be created):
//
// preprocessing solana txs -> preprocess txs
// processing txs 			-> process txs
// processing events        -> process events
//
// Errors types:
//
// missing account -> preprocess / process txs
// account data mismatch -> solana accounts -> preprocess / process txs
// account balance mismatch -> local data is not corresponding to onchain
//							-> solana accounts or inventory
//							-> process events
// missing balance -> process events (local inventory does not have enough balance to process an event)
//
// relations
// missing account -> related to tx
//                 -> can originate at any point when pre/processing a tx
// data mismatch -> related to tx
//				 -> can originate at any point durint pre/processing a tx
//	             -> or after a tx is preprocessed and balances/data is being validated with onchain
// balance mismatch -> related to tx
//					-> can originate only during validation after tx is preprocessed or after all events related to tx are processed
// missing balance -> can originate at any point during event processing when local inventory does not have enough balance to process the event
//
// Deduplication - errors should also be deduplicated, but all the occurences should be known
// missing account -> can only be created once per account per pre/process
// data mismatch -> once per account and data per pre/process
// balance mismatch -> once per account and balances per pre/process
// missing balance -> once per account and missing balance per process

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
	External       decimal.Decimal `json:"external"`
	Local          decimal.Decimal `json:"local"`
}

type ParserErrorAccountDataMismatch struct {
	AccountAddress string `json:"accountAddress"`
	Message        string `json:"message"`
}

type ParserErrorMissingBalance struct {
	AccountAddress string          `json:"accountAddresss"`
	Token          string          `json:"token"`
	Amount         decimal.Decimal `json:"amount"`
}

type ParserErrorType uint8

const (
	ParserErrorTypeMissingAccount ParserErrorType = iota
	// balances do not match between
	//  - inventory and chain
	//  - solana accounts and chain
	ParserErrorTypeAccountBalanceMismatch
	// account data does not match between
	//  - solana accounts and chain
	ParserErrorTypeAccountDataMismatch
	// missing balance in inventory
	ParserErrorTypeMissingBalance
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
