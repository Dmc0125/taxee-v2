package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"slices"
	rpcsolana "taxee/cmd/fetcher/solana"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Network string

const (
	NetworkSolana Network = "solana"
)

func NewNetwork(n string) (valid Network, ok bool) {
	ok = true
	switch n {
	case "solana":
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

func GetWallets(ctx context.Context, pool *pgxpool.Pool) ([]*WalletRow, error) {
	query := `
		select
			id, address, network, latest_tx_id 
		from
			wallet
	`
	rows, err := pool.Query(ctx, query)
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
	Instructions   []*SolanaInstruction            `json:"ixs"`
	NativeBalances map[string]*SolanaNativeBalance `json:"nativeBalances"`
	TokenBalances  map[string]*SolanaTokenBalances `json:"tokenBalances"`
	TokenDecimals  map[string]uint8                `json:"tokenDecimals"`
	BlockIndex     int32                           `json:"blockIndex"`
}

type TransactionRow struct {
	Id      string
	Err     bool
	Data    any
	Network Network
}

func GetTransactions(
	ctx context.Context,
	pool *pgxpool.Pool,
	walletsIds []int32,
) ([]*TransactionRow, error) {
	query := `
		select
			tx.id, tx.err, tx.data, tx.network
		from
			tx
		join
			tx_wallet_ref ref on
				ref.wallet_id = any($1) and
				ref.tx_id = tx.id
		-- where
		--	(tx.data->>'slot')::bigint > $2 or (
		--		(tx.data->>'slot')::bigint = $2 and
		--		(tx.data->>'blockIndex')::integer > $3
		--	)
		order by
			(tx.data->>'slot')::bigint asc,
			(tx.data->>'blockIndex')::integer asc
		-- limit
		--	$4
	`
	rows, err := pool.Query(ctx, query, walletsIds)
	if err != nil {
		return nil, fmt.Errorf("unable to query transactions: %w", err)
	}
	defer rows.Close()

	res := make([]*TransactionRow, 0)
	for rows.Next() {
		tx := TransactionRow{}
		var dataBytes []byte
		err := rows.Scan(&tx.Id, &tx.Err, &dataBytes, &tx.Network)
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

func InsertIteration(
	ctx context.Context,
	pool *pgxpool.Pool,
) (int32, error) {
	const query = "insert into iteration default values returning id"
	var id int32
	row := pool.QueryRow(ctx, query)
	err := row.Scan(&id)
	if err != nil {
		err = fmt.Errorf("unable to insert iteration: %w", err)
	}
	return id, err
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

type ErrAccountBalanceMissing struct {
	Expected uint64 `json:"expected"`
	Had      uint64 `json:"had"`
}

type NullJson struct {
	Valid bool
	Data  []byte
}

func (n NullJson) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Data, nil
}

func EnqueueInsertErr(
	batch *pgx.Batch,
	iterationId int32,
	txId string,
	ixIdx sql.NullInt32,
	idx int32,
	origin ErrOrigin,
	kind ErrType,
	address string,
	data NullJson,
) *pgx.QueuedQuery {
	const query = `
		insert into err (
			iteration_id, tx_id, ix_idx, idx, origin,
			type, address, data
		) values (
			$1, $2, $3, $4, $5, $6, $7, $8
		)
	`
	return batch.Queue(
		query,
		iterationId, txId, ixIdx, idx, origin,
		kind, address, data,
	)
}
