package db

import (
	"context"
	"database/sql/driver"
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

type Network uint16

const (
	NetworkSolana Network = iota

	// evm networks
	NetworkEvmStart
	NetworkEthereum
	NetworkArbitrum
	NetworkBsc
	NetworkAvaxC

	NetworksCount
)

func NewNetwork(n string) (valid Network, ok bool) {
	switch n {
	case "solana":
		return NetworkSolana, true
	case "arbitrum":
		return NetworkArbitrum, true
	case "bsc":
		return NetworkBsc, true
	case "avaxc":
		return NetworkAvaxC, true
	case "ethereum":
		return NetworkEthereum, true
	default:
		return 0, false
	}
}

func (nw *Network) String() string {
	switch *nw {
	case NetworkSolana:
		return "solana"
	case NetworkArbitrum:
		return "arbitrum"
	case NetworkBsc:
		return "bsc"
	case NetworkAvaxC:
		return "avaxc"
	case NetworkEthereum:
		return "ethereum"
	default:
		return ""
	}
}

func (dst *Network) Scan(src any) error {
	if src == nil {
		return nil
	}

	n, ok := src.(string)
	if !ok {
		return fmt.Errorf("invalid network type: %T", src)
	}

	*dst, ok = NewNetwork(n)
	if !ok {
		return fmt.Errorf("invalid network: %s", n)
	}

	return nil
}

func (nw Network) Value() (driver.Value, error) {
	v := nw.String()
	if v == "" {
		return nil, fmt.Errorf("invalid network: %d", nw)
	}
	return v, nil
}

type SolanaWalletData struct {
	LatestTxId string `json:"latestTxId"`
}

type EvmWalletData struct {
	TxsLatestHash                string `json:"txsLatestHash,omitempty"`
	TxsLatestBlockNumber         uint64 `json:"txsLatestBlockNumber,omitempty"`
	EventsLatestHash             string `json:"eventsLatestHash,omitempty"`
	EventsLatestBlockNumber      uint64 `json:"eventsLatestBlockNumber,omitempty"`
	InternalTxsLatestHash        string `json:"internalTxsLatestHash,omitempty"`
	InternalTxsLatestBlockNumber uint64 `json:"internalTxsLatestBlockNumber,omitempty"`
}

func DevSetWallet(
	ctx context.Context,
	pool *pgxpool.Pool,
	userAccountId int32,
	walletAddress string,
	network Network,
) (int32, []byte, error) {
	const q = `
		select
			wallet_id, wallet_data
		from
			dev_set_wallet($1, $2, $3)
	`
	row := pool.QueryRow(ctx, q, userAccountId, walletAddress, network)

	var walletId int32
	var walletData []byte
	if err := row.Scan(&walletId, &walletData); err != nil {
		return 0, nil, fmt.Errorf("unable to scan wallet: %w", err)
	}

	return walletId, walletData, nil
}

type WalletRow struct {
	Id      int32
	Address string
	Network Network
}

func GetWallets(ctx context.Context, pool *pgxpool.Pool, userAccountId int32) ([]*WalletRow, error) {
	query := `
		select
			id, address, network
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
		err := rows.Scan(&w.Id, &w.Address, &w.Network)
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

func DeleteWallet(batch *pgx.Batch, walletId, userAccountId int32) {
	// NOTE: want to keep message with errors
	const updateMessagesQuery = `
		update parser_message set 
			active = false,
			wallet_id = null
		where
			wallet_id = $1 and user_account_id = $2
	`
	batch.Queue(updateMessagesQuery, walletId, userAccountId)

	const deleteUserTransactionsQuery = `
		call dev_delete_user_transactions($1, $2)
	`
	batch.Queue(deleteUserTransactionsQuery, userAccountId, walletId)
	const deleteWalletQuery = `
		delete from wallet where 
			id = $1 and user_account_id = $2
	`
	batch.Queue(deleteWalletQuery, walletId, userAccountId)
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
	Signer         string                          `json:"string"`
	Accounts       []string                        `json:"accounts"`
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

func GetParsableTransactions(
	ctx context.Context,
	pool *pgxpool.Pool,
	userAccountId int32,
) ([]*TransactionRow, error) {
	query := `
		select	
			tx.id, tx.err, tx.data, tx.network, tx.timestamp
		from
			internal_tx itx
		join
			tx on tx.id = itx.tx_id
		where
			itx.user_account_id = $1
		order by
			itx.position asc
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
		case NetworkArbitrum, NetworkAvaxC, NetworkBsc, NetworkEthereum:
			var data EvmTransactionData
			if err = json.Unmarshal(dataBytes, &data); err != nil {
				return nil, fmt.Errorf("unable to unmarshal evm transaction data: %w", err)
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
