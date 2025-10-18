package db

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	rpcsolana "taxee/cmd/fetcher/solana"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

func InsertUserAccount(ctx context.Context, pool *pgxpool.Pool, name string) (int32, error) {
	const q = "insert into user_account (name) values ($1) returning id"
	row := pool.QueryRow(ctx, q, name)
	var id int32
	err := row.Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("unable to scan user: %w", err)
	}
	return id, nil
}

type GetUserAccountRow struct {
	Id   int32
	Name pgtype.Text
}

func GetUserAccount(ctx context.Context, pool *pgxpool.Pool, id int32) (*GetUserAccountRow, error) {
	const q = "select id, name from user_account where id = $1"
	row := pool.QueryRow(ctx, q, id)
	res := new(GetUserAccountRow)
	err := row.Scan(&res.Id, &res.Name)
	if err != nil {
		return nil, fmt.Errorf("unable to scan user account: %w", err)
	}
	return res, nil
}

// TODO: store as int or whatever, string is not needed
type Network string

const (
	NetworkSolana   Network = "solana"
	NetworkEthereum Network = "ethereum"
	NetworkArbitrum Network = "arbitrum"
)

func NewNetwork(n string) (valid Network, ok bool) {
	ok = true
	switch n {
	case "solana":
		valid = Network(n)
	case "arbitrum":
		valid = Network(n)
	default:
		ok = false
	}
	return
}

type WalletRow struct {
	Id                  int32
	Address             string
	Network             Network
	LatestTransactionId string
}

func GetWallets(ctx context.Context, pool *pgxpool.Pool, userAccountId int32) ([]*WalletRow, error) {
	query := `
		select
			id, address, network, latest_tx_id 
		from
			wallet
		where
			user_account_id = $1
	`
	rows, err := pool.Query(ctx, query, userAccountId)
	if err != nil {
		return nil, fmt.Errorf("unable to query wallets: %w", err)
	}

	res := make([]*WalletRow, 0)

	for rows.Next() {
		var w WalletRow
		err := rows.Scan(&w.Id, &w.Address, &w.Network, &w.LatestTransactionId)
		if err != nil {
			return nil, fmt.Errorf("unable to scan wallet: %w", err)
		}
		res = append(res, &w)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("unable to scan wallets: %w", err)
	}

	return res, nil
}

type Uint8Array []byte

func (b Uint8Array) MarshalJSON() ([]byte, error) {
	encoded := make([]byte, 0)
	encoded = append(encoded, '[')

	for i, c := range b {
		num := make([]byte, 0)
		for {
			rem := c % 10
			num = append(num, 48+rem)
			c /= 10
			if c == 0 {
				break
			}
		}
		slices.Reverse(num)
		encoded = append(encoded, num...)

		if i < len(b)-1 {
			encoded = append(encoded, ',')
		}
	}

	encoded = append(encoded, ']')
	return encoded, nil
}

type SolanaInnerInstruction struct {
	ProgramAddress string     `json:"programAddress"`
	Data           Uint8Array `json:"data"`
	Accounts       []string   `json:"accounts"`
	Logs           []string   `json:"logs"`
	ReturnData     []byte     `json:"returnData"`
}

type SolanaInstruction struct {
	*SolanaInnerInstruction
	InnerInstructions []*SolanaInnerInstruction `json:"innerInstructions"`
}

type SolanaTokenBalances rpcsolana.TokenBalance

type SolanaNativeBalance rpcsolana.NativeBalance

type SolanaTransactionData struct {
	Slot           uint64                          `json:"slot"`
	Fee            decimal.Decimal                 `json:"fee"`
	Instructions   []*SolanaInstruction            `json:"ixs"`
	NativeBalances map[string]*SolanaNativeBalance `json:"nativeBalances"`
	TokenBalances  map[string]*SolanaTokenBalances `json:"tokenBalances"`
	TokenDecimals  map[string]uint8                `json:"tokenDecimals"`
	BlockIndex     int32                           `json:"blockIndex"`
}

type EvmTransactionEvent struct {
	Address string        `json:"address"`
	Topics  [4]Uint8Array `json:"topics"`
	Data    Uint8Array    `json:"data"`
}

type EvmInternalTx struct {
	From            string          `json:"from"`
	To              string          `json:"to"`
	Value           decimal.Decimal `json:"value"`
	ContractAddress string          `json:"contractAddress"`
	Input           Uint8Array      `json:"input"`
}

type EvmTransactionData struct {
	Block       uint64                 `json:"block"`
	TxIdx       int32                  `json:"txIdx"`
	Input       Uint8Array             `json:"input"`
	Fee         decimal.Decimal        `json:"fee"`
	Value       decimal.Decimal        `json:"value"`
	From        string                 `json:"from"`
	To          string                 `json:"to"`
	Events      []*EvmTransactionEvent `json:"events"`
	InternalTxs []*EvmInternalTx       `json:"intenalTxs"`
}

type TransactionRow struct {
	Id        string
	Err       bool
	Data      any
	Network   Network
	Timestamp time.Time
}

func GetTransactions(
	ctx context.Context,
	pool *pgxpool.Pool,
	userAccountId int32,
) ([]*TransactionRow, error) {
	query := `
		select
			tx.id, tx.err, tx.data, tx.network, tx.timestamp
		from
			tx
		join
			tx_ref ref on
				ref.tx_id = tx.id and ref.user_account_id = $1
		order by
			(tx.data->>'slot')::bigint asc,
			(tx.data->>'blockIndex')::integer asc
	`
	rows, err := pool.Query(ctx, query, userAccountId)
	if err != nil {
		return nil, fmt.Errorf("unable to query transactions: %w", err)
	}
	defer rows.Close()

	res := make([]*TransactionRow, 0)
	for rows.Next() {
		tx := TransactionRow{}
		var dataBytes []byte
		err := rows.Scan(&tx.Id, &tx.Err, &dataBytes, &tx.Network, &tx.Timestamp)
		if err != nil {
			return nil, fmt.Errorf("unable to scan transaction: %w", err)
		}

		switch tx.Network {
		case NetworkSolana:
			var data SolanaTransactionData
			if err = json.Unmarshal(dataBytes, &data); err != nil {
				return nil, fmt.Errorf("unable to unmarshal transaction data: %w", err)
			}
			tx.Data = &data
		}

		res = append(res, &tx)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("unable to scan transacions: %w", err)
	}

	return res, nil
}

type ErrOrigin string

const (
	ErrOriginPreprocess ErrOrigin = "preparse"
	ErrOriginParse      ErrOrigin = "parse"
)

type ErrType string

const (
	ErrTypeAccountMissing         ErrType = "account_missing"
	ErrTypeAccountBalanceMismatch ErrType = "account_balance_mismatch"
)

type ErrAccountBalanceMismatch struct {
	Expected uint64 `json:"expected"`
	Had      uint64 `json:"had"`
}

func EnqueueInsertErr(
	batch *pgx.Batch,
	userAccountId int32,
	txId string,
	ixIdx pgtype.Int4,
	idx int32,
	origin ErrOrigin,
	kind ErrType,
	address string,
	data []byte,
) *pgx.QueuedQuery {
	const query = `
		insert into err (
			user_account_id, tx_id, ix_idx, idx, origin,
			type, address, data
		) values (
			$1, $2, $3, $4, $5, $6, $7, $8
		)
	`
	return batch.Queue(
		query,
		userAccountId, txId, ixIdx, idx, origin,
		kind, address, data,
	)
}
