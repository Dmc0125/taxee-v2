package fetcher

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"math/big"
	"slices"
	"taxee/cmd/fetcher/evm"
	"taxee/cmd/fetcher/solana"
	"taxee/cmd/parser"
	"taxee/pkg/assert"
	"taxee/pkg/db"
	"taxee/pkg/jsonrpc"
	"time"
	"unsafe"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

var (
	ERROR_INVALID_DATA = errors.New("invalid network or wallet data")
)

// insertTransactionQuery
//
//	insert into tx (
//		id, network, err, signer, fee_payer, timestamp, data
//	) values (
//		$1, $2, $3, $4, $5, $6, $7
//	) on conflict (id) do nothing
const insertTransactionQuery string = `
	insert into tx (
		id, network, err, signer, fee_payer, timestamp, data
	) values (
		$1, $2, $3, $4, $5, $6, $7
	) on conflict (id) do nothing
`

// setTransactionsForWallet
//
//	call set_transactions_for_wallet($1, $2, $3)
const setTransactionsForWallet string = `
	call set_transactions_for_wallet($1, $2, $3)
`

// setWalletDataQuery
//
//	update wallet set
//		data = data || $1::jsonb
//	where
//		id = $2
const setWalletDataQuery string = `
	update wallet set
		data = data || $1::jsonb
	where
		id = $2
`

// setLatestSolanaRelatedAccountTxId
//
//	update solana_related_account set
//		latest_tx_id = $1
//	where wallet_id = $2 and address = $3
const setLatestSolanaRelatedAccountTxId string = `
	update solana_related_account set
		latest_tx_id = $1
	where wallet_id = $2 and address = $3
`

type solanaAccount struct {
	solanaRelatedAccount
	related bool
}

type solanaAccounts []solanaAccount

func (accounts *solanaAccounts) Append(newAccount string) {
	for _, a := range *accounts {
		if a.Address == newAccount {
			return
		}
	}
	*accounts = append(*accounts, solanaAccount{
		solanaRelatedAccount: solanaRelatedAccount{Address: newAccount},
		related:              true,
	})
}

func fetchSolanaAccount(
	ctx context.Context,
	pool *pgxpool.Pool,
	rpcUrl string,
	accounts *[]solanaAccount,
	userAccountId,
	walletId int32,
	account solanaAccount,
) error {
	const LIMIT = 1000
	before, newLastestTxId := "", ""
	signatures := make([]string, 0)

	for {
		slog.Info("Fetching signatures", "beforeSignature", before)
		var result solana.GetSignaturesForAddressResult
		err := jsonrpc.Call(
			ctx, rpcUrl,
			solana.GetSignaturesForAddress,
			&solana.GetSignaturesForAddressParams{
				Address: account.Address,
				Config: &solana.GetSignaturesForAddressConfig{
					Commitment: solana.CommitmentConfirmed,
					Limit:      LIMIT,
					Before:     before,
					Until:      account.LatestTxId,
				},
			},
			&result,
			true,
		)
		if err != nil {
			return fmt.Errorf("unable to fetch signatures for address: %w", err)
		}

		slog.Info("fetched signatures", "count", len(result))
		for _, s := range result {
			signatures = append(signatures, s.Signature)
		}

		if len(result) > 0 && newLastestTxId == "" {
			newLastestTxId = result[0].Signature
		}

		if len(result) < LIMIT {
			break
		}

		before = result[len(result)-1].Signature
	}

	if len(signatures) == 0 {
		slog.Info("no new signatures since last fetch")
		return nil
	}

	slog.Info("fetched signatures", "count", len(signatures))
	dbBatch := pgx.Batch{}

	processTx := func(txResult *solana.GetTransactionResult) error {
		compiledTx, err := solana.ParseTransaction(txResult.Transaction[0])
		if err != nil {
			return fmt.Errorf("unbale to parse solana transaction: %w", err)
		}
		tx, err := solana.DecompileTransaction(
			txResult.Slot,
			txResult.BlockTime,
			txResult.Meta,
			compiledTx,
		)
		if err != nil {
			return fmt.Errorf("unable to decompile solana transactions: %w", err)
		}

		ixs := make([]*db.SolanaInstruction, len(tx.Ixs))
		for i, ix := range tx.Ixs {
			iixs := make([]*db.SolanaInnerInstruction, len(ix.InnerInstructions))
			for j, iix := range ix.InnerInstructions {
				iixs[j] = &db.SolanaInnerInstruction{
					ProgramAddress: iix.ProgramAddress,
					Data:           db.Uint8Array(iix.Data),
					Accounts:       iix.Accounts,
					Logs:           iix.Logs,
					ReturnData:     iix.ReturnData,
				}
			}

			ixs[i] = &db.SolanaInstruction{
				InnerInstructions: iixs,
				SolanaInnerInstruction: &db.SolanaInnerInstruction{
					ProgramAddress: ix.ProgramAddress,
					Data:           db.Uint8Array(ix.Data),
					Accounts:       ix.Accounts,
					Logs:           ix.Logs,
					ReturnData:     ix.ReturnData,
				},
			}
		}

		type nativeBalances = map[string]*db.SolanaNativeBalance
		type tokenBalances = map[string]*db.SolanaTokenBalances

		fee := decimal.NewFromBigInt(
			new(big.Int).SetUint64(tx.Fee),
			// NOTE: using 9 because only svm chain we support now
			// is solana
			-9,
		)

		txData := db.SolanaTransactionData{
			Slot:           tx.Slot,
			Fee:            fee,
			Instructions:   ixs,
			NativeBalances: *(*nativeBalances)(unsafe.Pointer(&tx.NativeBalances)),
			TokenBalances:  *(*tokenBalances)(unsafe.Pointer(&tx.TokenBalances)),
			TokenDecimals:  tx.TokenDecimals,
			BlockIndex:     -1,
			Signer:         tx.Accounts[0],
			Accounts:       tx.Accounts,
		}

		if !account.related && !tx.Err {
			parser.RelatedAccountsFromTx(
				(*solanaAccounts)(accounts),
				account.Address,
				&txData,
			)
		}

		marshalled, err := json.Marshal(txData)
		assert.NoErr(err, "unable to marshal solana tx data")

		dbBatch.Queue(
			insertTransactionQuery,
			tx.Signature, db.NetworkSolana, tx.Err,
			tx.Accounts[0], tx.Accounts[0], tx.Blocktime,
			marshalled,
		)

		return nil
	}

	rpcBatch := jsonrpc.NewBatch(rpcUrl)

	for _, s := range signatures {
		rpcBatch.Queue(
			solana.GetTransaction,
			&solana.GetTransactionParams{
				Signature: s,
				Config: &solana.GetTransactionConfig{
					Commitment:                     solana.CommitmentConfirmed,
					MaxSupportedTransactionVersion: 0,
					Encoding:                       solana.EncodingBase64,
				},
			},
			func(rm json.RawMessage) (retry bool, err error) {
				if string(rm) == "null" {
					return true, nil
				}

				var result solana.GetTransactionResult
				if err := json.Unmarshal(rm, &result); err != nil {
					return false, err
				}
				if err := result.Validate(); err != nil {
					return false, err
				}

				return false, processTx(&result)
			},
		)
	}

	slog.Info("sending get transactions batch", "count", rpcBatch.Len())
	if err := jsonrpc.CallBatch(ctx, rpcBatch); err != nil {
		return fmt.Errorf("unable to fetch transactions: %w", err)
	}

	if dbBatch.Len() > 0 {
		if err := pool.SendBatch(ctx, &dbBatch).Close(); err != nil {
			return fmt.Errorf("unable to insert txs: %w", err)
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("unable to begin tx: %w", err)
		}
		defer tx.Rollback(ctx)

		switch account.related {
		case true:
			_, err := tx.Exec(
				ctx, "call set_transactions_for_related_account($1, $2, $3, $4)",
				userAccountId, signatures, walletId, pgtype.Text{Valid: true, String: account.Address},
			)
			if err != nil {
				return fmt.Errorf("unable to transactions for related account: %w", err)
			}

			slog.Info("updating latest account signature", "signature", newLastestTxId)
			_, err = tx.Exec(
				ctx, setLatestSolanaRelatedAccountTxId,
				newLastestTxId, walletId, account.Address,
			)
			if err != nil {
				return fmt.Errorf("unable to set latest tx id for solana related account: %w", err)
			}
		default:
			_, err := tx.Exec(
				ctx, setTransactionsForWallet,
				userAccountId, signatures, walletId,
			)
			if err != nil {
				return fmt.Errorf("unable to transactions for wallet: %w", err)
			}

			walletData, err := json.Marshal(db.SolanaWalletData{LatestTxId: newLastestTxId})
			assert.NoErr(err, "")

			slog.Info("updating latest wallet signature", "signature", newLastestTxId)
			_, err = tx.Exec(ctx, setWalletDataQuery, walletData, walletId)
			if err != nil {
				return fmt.Errorf("unable to set latest tx id for solana wallet: %w", err)
			}
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("unable to commit tx: %w", err)
		}
	}

	return nil
}

type solanaRelatedAccount struct {
	Address    string
	LatestTxId string
}

type getSolanaTxsWithDuplicateSlotsRow struct {
	Slot int64
	Id   string
}

func getSolanaTxsWithDuplicateSlots(
	ctx context.Context,
	pool *pgxpool.Pool,
	limit int,
) ([]*getSolanaTxsWithDuplicateSlotsRow, error) {
	const query = `
		select t.slot, t.id from (
			select
				(data->>'slot')::bigint as slot,
				id,
				count(*) over (
					partition by data->>'slot'
				) as dup_count
			from
				tx
			where
				network = 'solana' and (data->>'blockIndex')::integer = -1
		) t
		where 
			t.dup_count > 1
		limit
			$1
	`
	rows, err := pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("unable to get query solana txs with duplicate slots: %w", err)
	}

	res := make([]*getSolanaTxsWithDuplicateSlotsRow, 0)

	for rows.Next() {
		var tx getSolanaTxsWithDuplicateSlotsRow
		err := rows.Scan(&tx.Slot, &tx.Id)
		if err != nil {
			return nil, fmt.Errorf("unable to scan solana tx: %w", err)
		}
		res = append(res, &tx)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("unable to scan solana txs: %w", err)
	}

	return res, nil
}

// Fetches related accounts and transactions for solana wallet
//
// 2 modes: normal and fresh
//
//   - normal mode: starts from previous checkpoints, does not change what was
//     fetched
//   - fresh mode: deletes all accounts, transactions and sets tx count to 0,
//     then refetches all from start
//
// right now, this function commits updates to db progressively, immediately
// when it can, so everytime it is called, there may be new transactions
// inserted
//
// when canceled, the execution just stops and does not rollback what was
// already saved which means, wallet for which fetch was canceled, most likely
// has non sensical data related to it
func fetchSolanaWallet(
	ctx context.Context,
	pool *pgxpool.Pool,
	rpcUrl string,
	userAccountId,
	walletId int32,
	walletAddress string,
	latestTxId pgtype.Text,
	fresh bool,
) error {
	accounts := make([]solanaAccount, 1)

	if fresh {
		accounts[0] = solanaAccount{
			solanaRelatedAccount{
				Address:    walletAddress,
				LatestTxId: "",
			},
			false,
		}

		batch := pgx.Batch{}

		// deletes related account and tx refs related to them
		const deleteRelatedAccountsQuery = `
			delete from solana_related_account where
				user_account_id = $1 and wallet_id = $2
		`
		const deleteWalletTxRefsQuery = `
			delete from tx_ref where
				user_account_id = $1 and wallet_id = $2
		`
		const updateWalletTxCountQuery = `
			update wallet set
				tx_count = 0
			where
				user_account_id = $1 and id = $2
		`
		batch.Queue(deleteRelatedAccountsQuery, userAccountId, walletId)
		batch.Queue(deleteWalletTxRefsQuery, userAccountId, walletId)
		batch.Queue(updateWalletTxCountQuery, userAccountId, walletId)
		if err := pool.SendBatch(ctx, &batch).Close(); err != nil {
			return fmt.Errorf("unable to delete wallet transactions: %w", err)
		}
	} else {
		accounts[0] = solanaAccount{
			solanaRelatedAccount{
				Address:    walletAddress,
				LatestTxId: latestTxId.String,
			},
			false,
		}

		const getSolanaRelatedAccounts = `
			select address, latest_tx_id from solana_related_account where
				user_account_id = $1 and wallet_id = $2
		`
		rows, err := pool.Query(
			ctx, getSolanaRelatedAccounts,
			userAccountId, walletId,
		)
		if err != nil {
			return fmt.Errorf("unable to query solana related accounts: %w", err)
		}

		for rows.Next() {
			var acc solanaRelatedAccount
			if err := rows.Scan(&acc.Address, &acc.LatestTxId); err != nil {
				return fmt.Errorf("unable to scan solana related accounts: %w", err)
			}
			accounts = append(accounts, solanaAccount{acc, true})
		}

		if err := rows.Err(); err != nil {
			return fmt.Errorf("unable to read solana related accounts: %w", err)
		}
	}

	accountsCount := 0
	for len(accounts) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		accountsCount += 1

		acc := accounts[0]
		slog.Info(
			"fetching solana account",
			"address", acc.Address,
			"related", acc.related,
			"latestSignature", acc.LatestTxId,
		)

		err := fetchSolanaAccount(
			ctx, pool, rpcUrl, &accounts,
			userAccountId, walletId, acc,
		)
		if err != nil {
			return err
		}
		accounts = accounts[1:]
	}

	slog.Info(
		"Txs fetched - starting slots deduplication",
		"accounts", accountsCount,
	)

	const LIMIT = 100

	for {
		dups, err := getSolanaTxsWithDuplicateSlots(ctx, pool, LIMIT)
		if err != nil {
			return fmt.Errorf("unable to get solana txs with duplicate slots: %w", err)
		}

		slots := make(map[int64][]string)
		for _, tx := range dups {
			slots[tx.Slot] = append(slots[tx.Slot], tx.Id)
		}

		if len(slots) == 0 {
			break
		}

		slog.Info("need to fetch blocks", "count", len(slots))

		rpcBatch := jsonrpc.NewBatch(rpcUrl)
		dbBatch := pgx.Batch{}

		for slot, signatures := range slots {
			params := solana.GetBlockParams{
				Slot: uint64(slot),
				Config: &solana.GetBlockConfig{
					Commitment:                     solana.CommitmentConfirmed,
					TransactionDetails:             solana.TransactionDetailsSignatures,
					Encoding:                       solana.EncodingBase64,
					MaxSupportedTransactionVersion: 0,
				},
			}
			rpcBatch.Queue(
				solana.GetBlock, &params,
				func(rm json.RawMessage) (retry bool, err error) {
					if string(rm) == "null" {
						slog.Error("get block result empty")
						return true, nil
					}

					var result solana.GetBlockResult
					if err := json.Unmarshal(rm, &result); err != nil {
						return false, fmt.Errorf("unable to unmarshal get block result: %w", err)
					}
					if err := result.Validate(); err != nil {
						return false, fmt.Errorf("invalid get block result: %w", err)
					}

					for _, s := range signatures {
						idx := slices.IndexFunc(result.Signatures, func(blockSignature string) bool {
							return s == blockSignature
						})
						if idx != -1 {
							const query = `
								update tx set
									data = data || jsonb_build_object('blockIndex', $1::integer)
								where
									id = $2
							`
							dbBatch.Queue(query, int32(idx), s)
						} else {
							return false, fmt.Errorf("unable to find signature in block: %s", s)
						}
					}

					return false, nil
				},
			)
		}

		if err := jsonrpc.CallBatch(ctx, rpcBatch); err != nil {
			return fmt.Errorf("unable to send get blocks batch: %w", err)
		}

		br := pool.SendBatch(ctx, &dbBatch)
		if err := br.Close(); err != nil {
			return fmt.Errorf("unable to update solana txs with duplicate slots: %w", err)
		}

		if len(dups) < LIMIT {
			break
		}
	}

	slog.Info("fetching done")
	return nil
}

func fetchEvmWallet(
	ctx context.Context,
	pool *pgxpool.Pool,
	client *evm.Client,
	alchemyApiKey string,
	userAccountId,
	walletId int32,
	walletAddress string,
	network db.Network,
	walletData *db.EvmWalletData,
) error {
	chainId, nativeDecimals, err := evm.ChainIdAndNativeDecimals(network)
	if err != nil {
		return err
	}
	rpcUrl, err := evm.AlchemyApiUrl(network, alchemyApiKey)
	if err != nil {
		return err
	}

	calcFeePaid := func(gasUsed, gasPrice *big.Int) decimal.Decimal {
		g := decimal.NewFromBigInt(
			gasUsed,
			-int32(nativeDecimals),
		)
		gp := decimal.NewFromBigInt(gasPrice, 0)
		return g.Mul(gp)
	}

	type evmTx struct {
		data          *db.EvmTransactionData
		timestamp     time.Time
		err           bool
		fetchNormalTx bool
		gasPrice      *big.Int
	}
	fetchedTxs := make(map[evm.Hash]*evmTx)

	const endBlock = uint64(math.MaxInt64)

	//////////////////
	// fetch all normal transactions
	txsStartBlock := walletData.TxsLatestBlockNumber
	txsStartTxHash := walletData.TxsLatestHash

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		slog.Info("fetching normal txs", "startblock", txsStartBlock)
		txs, err := client.GetWalletNormalTransactions(
			chainId, walletAddress,
			txsStartBlock, endBlock,
		)
		if err != nil {
			return fmt.Errorf("unable to fetch normal transactions: %w", err)
		}

		// TODO: find out how etherscan handles paginatino, are blocks inclusive
		// or not, limits, ....

		// NOTE: this is sufficient, because the start tx hash has the same
		// block as start block and the start block is used in the request
		for i, tx := range txs {
			if tx.Hash == txsStartTxHash {
				txs = txs[i+1:]
				break
			}
		}

		if len(txs) == 0 {
			break
		}

		for _, tx := range txs {
			fee := calcFeePaid((*big.Int)(tx.GasUsed), (*big.Int)(tx.GasPrice))
			value := decimal.NewFromBigInt((*big.Int)(tx.Value), -int32(nativeDecimals))

			fetchedTxs[evm.Hash(tx.Hash)] = &evmTx{
				data: &db.EvmTransactionData{
					Block: uint64(tx.BlockNumber),
					TxIdx: int32(tx.TxIdx),
					Input: db.Uint8Array(tx.Input),
					Fee:   fee,
					Value: value,
					From:  tx.From,
					To:    tx.To,
				},
				timestamp: time.Time(tx.Timestamp),
			}
		}

		last := txs[len(txs)-1]
		txsStartBlock = uint64(last.BlockNumber)
		txsStartTxHash = last.Hash

	}

	//////////////////
	// fetch internal txs
	internalTxsStartBlock := walletData.InternalTxsLatestBlockNumber
	internalTxsStartTxHash := walletData.InternalTxsLatestHash

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		slog.Info("fetching internal txs", "startblock", internalTxsStartBlock)
		txs, err := client.GetInternalTransactionsByAddress(
			chainId, walletAddress,
			internalTxsStartBlock, endBlock,
		)
		if err != nil {
			return fmt.Errorf("unable to fetch internal transactions: %w", err)
		}

		for i, tx := range txs {
			if tx.Hash == internalTxsStartTxHash {
				txs = txs[i+1:]
				break
			}
		}

		if len(txs) == 0 {
			break
		}

		for _, tx := range txs {
			if _, ok := fetchedTxs[evm.Hash(tx.Hash)]; !ok {
				fetchedTxs[evm.Hash(tx.Hash)] = &evmTx{
					fetchNormalTx: true,
					timestamp:     time.Time(tx.Timestamp),
				}
			}
		}

		last := txs[len(txs)-1]
		internalTxsStartBlock = uint64(last.BlockNumber)
		internalTxsStartTxHash = last.Hash
	}

	//////////////////
	// fetch incoming token transfers
	transferTopic, _ := hex.DecodeString("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

	walletBytes, err := hex.DecodeString(walletAddress[2:])
	assert.NoErr(err, fmt.Sprintf("invalid evm walletAddress: %s", walletAddress))

	addressTopic := [32]byte{}
	copy(addressTopic[32-len(walletBytes):], walletBytes)
	topics := [4][]byte{
		transferTopic, nil,
		addressTopic[:], nil,
	}

	topicsOperators := [6]evm.TopicOperator{}
	topicsOperators[3] = evm.TopicAnd

	eventsStartBlock := walletData.EventsLatestBlockNumber
	eventsStartTxHash := walletData.EventsLatestHash

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		slog.Info("fetching incoming transfers", "startblock", eventsStartBlock)
		events, err := client.GetEventLogsByTopics(
			chainId,
			eventsStartBlock, endBlock,
			topics, topicsOperators,
		)
		if err != nil {
			return fmt.Errorf("unable to fetch event logs by topics: %w", err)
		}

		for i := len(events) - 1; i >= 0; i -= 1 {
			e := events[i]
			if e.Hash == eventsStartTxHash {
				events = events[i+1:]
				break
			}
		}

		if len(events) == 0 {
			break
		}

		for _, event := range events {
			if _, ok := fetchedTxs[evm.Hash(event.Hash)]; !ok {
				fetchedTxs[evm.Hash(event.Hash)] = &evmTx{
					fetchNormalTx: true,
					timestamp:     time.Time(event.Timestamp),
				}
			}
		}

		last := events[len(events)-1]
		eventsStartBlock = uint64(last.BlockNumber)
		eventsStartTxHash = last.Hash
	}

	if len(fetchedTxs) == 0 {
		slog.Info("fetching done - no new txs found")
		return nil
	}

	type messageInternalTxs struct {
		hash        string
		internalTxs []*db.EvmInternalTx
	}
	chanInternalTxs := make(chan any)
	allHashes := slices.Collect(maps.Keys(fetchedTxs))

	go func() {
		for i, hash := range allHashes {
			select {
			case <-ctx.Done():
				chanInternalTxs <- ctx.Err()
				close(chanInternalTxs)
				return
			default:
			}

			internalTxs, err := client.GetInternalTransactionsByHash(chainId, string(hash))
			if err != nil {
				chanInternalTxs <- fmt.Errorf("unable to get internal tx by hash: %w", err)
				close(chanInternalTxs)
				return
			}

			m := messageInternalTxs{
				hash: string(hash),
			}

			for _, internalTx := range internalTxs {
				value := decimal.NewFromBigInt(
					(*big.Int)(internalTx.Value),
					-int32(nativeDecimals),
				)
				m.internalTxs = append(
					fetchedTxs[hash].data.InternalTxs,
					&db.EvmInternalTx{
						From:            internalTx.From,
						To:              internalTx.To,
						Value:           value,
						ContractAddress: internalTx.ContractAddress,
						Input:           db.Uint8Array(internalTx.Input),
					},
				)
			}

			chanInternalTxs <- &m
			if i == len(allHashes)-1 {
				close(chanInternalTxs)
			}
		}
	}()

	rpcBatch := jsonrpc.NewBatch(rpcUrl)

	for hash, tx := range fetchedTxs {
		params := evm.GetTransactionParams(hash)

		if tx.fetchNormalTx {
			rpcBatch.Queue(
				evm.GetTransactionByHash, &params,
				func(rm json.RawMessage) (retry bool, err error) {
					if string(rm) == "null" {
						return true, nil
					}

					var result evm.GetTransactionByHashResult
					if err := json.Unmarshal(rm, &result); err != nil {
						return false, fmt.Errorf("unable to unmarshal get transaction result: %w", err)
					}

					if err := result.Validate(); err != nil {
						return false, err
					}

					value := decimal.NewFromBigInt(
						(*big.Int)(&result.Value),
						-int32(nativeDecimals),
					)
					fetchedTxs[result.Hash].data = &db.EvmTransactionData{
						Block: uint64(result.BlockNumber),
						TxIdx: int32(result.TransactionIndex),
						Input: db.Uint8Array(result.Input),
						Value: value,
						From:  string(result.From),
						To:    string(result.To),
					}
					fetchedTxs[result.Hash].gasPrice = (*big.Int)(&result.GasPrice)

					return false, nil
				},
			)
		}

		rpcBatch.Queue(
			evm.GetTransactionReceipt, &params,
			func(rm json.RawMessage) (retry bool, err error) {
				if string(rm) == "null" {
					return true, nil
				}

				var result evm.GetTransactionReceiptResult
				if err := json.Unmarshal(rm, &result); err != nil {
					return false, fmt.Errorf("unable to unmarshal get transaction receipt result: %w", err)
				}
				if err := result.Validate(); err != nil {
					return false, err
				}

				tx := fetchedTxs[result.TransactionHash]
				tx.err = result.Status == 0

				if tx.gasPrice != nil {
					tx.data.Fee = calcFeePaid((*big.Int)(&result.GasUsed), tx.gasPrice)
				}

				for _, log := range result.Logs {
					var topics [4]db.Uint8Array
					for i, topic := range log.Topics {
						topics[i] = db.Uint8Array(topic[:])
					}

					tx.data.Events = append(
						tx.data.Events,
						&db.EvmTransactionEvent{
							Address: string(log.Address),
							Topics:  topics,
							Data:    db.Uint8Array(log.Data),
						},
					)
				}

				return false, nil
			},
		)
	}

	slog.Info("sending rpc batch", "requests", rpcBatch.Len())
	if err := jsonrpc.CallBatch(ctx, rpcBatch); err != nil {
		return fmt.Errorf("unable to send rpc batch: %w", err)
	}

	for i := 1; ; i += 1 {
		message, ok := <-chanInternalTxs
		if !ok {
			break
		}
		switch m := message.(type) {
		case error:
			return m
		case *messageInternalTxs:
			slog.Info("fetched internal transaction", "idx", i, "total", len(fetchedTxs))
			tx := fetchedTxs[evm.Hash(m.hash)]
			tx.data.InternalTxs = m.internalTxs
		}
	}

	err = db.ExecuteTx(ctx, pool, func(ctx context.Context, tx pgx.Tx) error {
		batch := pgx.Batch{}

		for hash, tx := range fetchedTxs {
			dataSerialized, err := json.Marshal(tx.data)
			if err != nil {
				return fmt.Errorf("unable to marshal evm tx data: %w", err)
			}

			batch.Queue(
				insertTransactionQuery,
				hash, network, tx.err, tx.data.From, tx.data.From, tx.timestamp,
				dataSerialized,
			)
		}

		batch.Queue(
			setTransactionsForWallet,
			userAccountId, allHashes, walletId,
		)

		newWalletDataMarshaled, err := json.Marshal(db.EvmWalletData{
			TxsLatestHash:                txsStartTxHash,
			TxsLatestBlockNumber:         txsStartBlock,
			EventsLatestHash:             eventsStartTxHash,
			EventsLatestBlockNumber:      eventsStartBlock,
			InternalTxsLatestHash:        internalTxsStartTxHash,
			InternalTxsLatestBlockNumber: internalTxsStartBlock,
		})
		if err != nil {
			return fmt.Errorf("unable to marshal evm wallet data: %w", err)
		}

		batch.Queue(
			setWalletDataQuery,
			newWalletDataMarshaled, walletId,
		)

		if err := tx.SendBatch(ctx, &batch).Close(); err != nil {
			return fmt.Errorf("unable to execute evm batch: %w", err)
		}

		return nil
	})

	// TODO: later can validate balances at the end of the block with
	// eth_getBalance
	// eth_getStorageAt or eth_call ??
	slog.Info("fetching done")
	return err
}

func Fetch(
	ctx context.Context,
	pool *pgxpool.Pool,
	etherscanClient *evm.Client,
	alchemyApiKey string,
	//
	userAccountId int32,
	network db.Network,
	walletAddress string,
	walletId int32,
	walletDataSerialized json.RawMessage,
	fresh bool,
) error {
	slog.Info(
		"Starting wallet fetch",
		"user", userAccountId,
		"wallet", walletId,
		"address", walletAddress,
		"network", network,
		"fresh", fresh,
	)

	switch {
	case network == db.NetworkSolana:
		var walletData db.SolanaWalletData
		if err := json.Unmarshal(walletDataSerialized, &walletData); err != nil {
			return fmt.Errorf(
				"%w: invalid wallet data for solana: %w",
				ERROR_INVALID_DATA, err,
			)
		}

		latestTxId := pgtype.Text{}
		if walletData.LatestTxId != "" {
			latestTxId.Valid = true
			latestTxId.String = walletData.LatestTxId
		}

		alchemyRpcUrl := fmt.Sprintf("%s%s", solana.AlchemyApiUrl, alchemyApiKey)

		return fetchSolanaWallet(
			ctx, pool, alchemyRpcUrl,
			userAccountId, walletId, walletAddress, latestTxId, fresh,
		)
	case network > db.NetworkEvmStart:
		var walletData db.EvmWalletData
		if err := json.Unmarshal(walletDataSerialized, &walletData); err != nil {
			return fmt.Errorf(
				"%w: invalid wallet data for EVM (%s): %w",
				ERROR_INVALID_DATA, network.String(), err,
			)
		}

		if fresh {
			walletData = db.EvmWalletData{}
		}

		return fetchEvmWallet(
			ctx, pool, etherscanClient, alchemyApiKey,
			userAccountId, walletId, walletAddress, network, &walletData,
		)
	default:
		return fmt.Errorf("%w: invalid network", ERROR_INVALID_DATA)
	}
}
