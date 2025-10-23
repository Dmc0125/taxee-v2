package fetcher

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"slices"
	"taxee/cmd/fetcher/evm"
	"taxee/cmd/fetcher/solana"
	"taxee/cmd/parser"
	"taxee/pkg/assert"
	"taxee/pkg/db"
	"taxee/pkg/logger"
	"time"
	"unsafe"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

func enqueueInsertTx(
	batch *pgx.Batch,
	txId string,
	network db.Network,
	err bool,
	signer, feePayer string,
	timestamp time.Time,
	txDataMarshalled []byte,
) *pgx.QueuedQuery {
	const query = `
		insert into tx (
			id, network, err, signer, fee_payer, timestamp, data
		) values (
			$1, $2, $3, $4, $5, $6, $7
		) on conflict (id) do nothing
	`
	return batch.Queue(
		query,
		txId,
		network,
		err,
		signer,
		feePayer,
		timestamp,
		txDataMarshalled,
	)
}

func setUserTransactions(
	ctx context.Context,
	pool *pgxpool.Pool,
	userAccountId int32,
	txIds []string,
	walletId int32,
	relatedAccountAddress pgtype.Text,
) error {
	query := "call set_user_transactions($1, $2, $3, $4)"
	_, err := pool.Exec(ctx, query, userAccountId, txIds, walletId, relatedAccountAddress)
	if err != nil {
		return fmt.Errorf("unable to query set_user_transactions: %w", err)
	}
	return nil
}

func setWalletData(
	ctx context.Context,
	pool *pgxpool.Pool,
	walletId int32,
	data any,
) error {
	const query = `
		update 
			wallet 
		set
			data = data || $1::jsonb
		where
			id = $2
	`

	d, err := json.Marshal(data)
	assert.NoErr(err, "")

	if _, err = pool.Exec(ctx, query, d, walletId); err != nil {
		return fmt.Errorf("unable to set latest wallet tx id: %w", err)
	}

	return nil
}

func setLatestSolanaRelatedAccountTxId(
	ctx context.Context,
	pool *pgxpool.Pool,
	walletId int32,
	address, latestTxId string,
) error {
	query := `
		update solana_related_account set
			latest_tx_id = $1
		where wallet_id = $2 and address = $3
	`
	_, err := pool.Exec(ctx, query, latestTxId, walletId, address)
	if err != nil {
		return fmt.Errorf("unable to set latest solana related account tx id: %w", err)
	}
	return nil
}

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
	rpc *solana.Rpc,
	accounts *[]solanaAccount,
	userAccountId,
	walletId int32,
	account solanaAccount,
) {
	logger.Info("Starting fetching transactions for address: %s", account.Address)
	relatedAccountAddress := pgtype.Text{
		Valid:  account.related,
		String: account.Address,
	}

	const LIMIT = 1000
	before, newLastestTxId := "", ""
	retry := make([]string, 0)

	for {
		logger.Info("Fetching signatures from: \"%s\"", before)
		signaturesResponse, err := rpc.GetSignaturesForAddress(
			account.Address,
			&solana.GetSignaturesForAddressParams{
				Commitment: solana.CommitmentConfirmed,
				Limit:      LIMIT,
				Before:     before,
				Until:      account.LatestTxId,
			},
		)
		assert.NoErr(err, "unable to get signatures for address")
		if signaturesResponse.Error != nil {
			assert.True(
				false,
				"unable to get signatures for address: %#v",
				*signaturesResponse.Error,
			)
		}
		logger.Info("Fetched %d signatures ", len(signaturesResponse.Result))

		if len(signaturesResponse.Result) > 0 {
			if newLastestTxId == "" {
				newLastestTxId = signaturesResponse.Result[0].Signature
			}
			before = signaturesResponse.Result[len(signaturesResponse.Result)-1].Signature
		}

		for len(retry) > 0 {
			signaturesResponse.Result = append(
				signaturesResponse.Result,
				&solana.GetSignaturesForAddressResponse{
					Signature: retry[0],
				},
			)
			retry = retry[1:]
		}

		if len(signaturesResponse.Result) == 0 {
			break
		}

		const BATCH_SIZE = 10
		queue := pgx.Batch{}
		txIds := make([]string, 0)

		logger.Info("Txs chunks %d", len(signaturesResponse.Result)/BATCH_SIZE+1)
		chunkIdx := 0

		for chunk := range slices.Chunk(signaturesResponse.Result, BATCH_SIZE) {
			chunkIdx += 1
			batch := make([]*solana.GetMultipleTransactionsParams, len(chunk))
			for i, sig := range chunk {
				batch[i] = &solana.GetMultipleTransactionsParams{
					Signature:  sig.Signature,
					Commitment: solana.CommitmentConfirmed,
				}
			}

			batchRes, err := rpc.GetMultipleTransactions(batch)
			assert.NoErr(err, "unable to get multiple transactions")
			logger.Info("Fetched txs chunk %d", chunkIdx)

			for i, res := range batchRes {
				switch {
				case res.Error != nil:
					logger.Error("Tx response err")
					retry = append(retry, batch[i].Signature)
				default:
					tx := res.Result

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

					enqueueInsertTx(
						&queue,
						tx.Signature,
						db.NetworkSolana,
						tx.Err,
						tx.Accounts[0],
						tx.Accounts[0],
						tx.Blocktime,
						marshalled,
					)
					txIds = append(txIds, tx.Signature)
				}
			}
		}

		if queue.Len() > 0 {
			br := pool.SendBatch(ctx, &queue)
			for range queue.Len() {
				_, err := br.Exec()
				assert.NoErr(err, "unable to insert tx")
			}
			br.Close()

			err := setUserTransactions(
				ctx,
				pool,
				userAccountId,
				txIds,
				walletId,
				relatedAccountAddress,
			)
			assert.NoErr(err, "")
		}

		if len(signaturesResponse.Result) < LIMIT && len(retry) == 0 {
			break
		}
	}

	if newLastestTxId == "" {
		return
	}

	var err error
	if relatedAccountAddress.Valid {
		err = setLatestSolanaRelatedAccountTxId(
			ctx, pool, walletId, relatedAccountAddress.String, newLastestTxId,
		)
		logger.Info(
			"Set latest txId \"%s\" for related account %s",
			newLastestTxId,
			relatedAccountAddress.String,
		)
	} else {
		err = setWalletData(ctx, pool, walletId, db.SolanaWalletData{newLastestTxId})
		logger.Info(
			"Set latest txId \"%s\" for wallet %s",
			newLastestTxId,
			account.Address,
		)
	}
	assert.NoErr(err, "")
}

type solanaRelatedAccount struct {
	Address    string
	LatestTxId string
}

func getSolanaRelatedAccounts(
	ctx context.Context,
	pool *pgxpool.Pool,
	walletId int32,
) ([]*solanaRelatedAccount, error) {
	const query = `
		select 
			address, latest_tx_id
		from
			solana_related_account
		where
			wallet_id = $1
	`
	rows, err := pool.Query(ctx, query, walletId)
	if err != nil {
		return nil, fmt.Errorf("unable to query solana accounts: %w", err)
	}

	res := make([]*solanaRelatedAccount, 0)
	for rows.Next() {
		acc := new(solanaRelatedAccount)
		err := rows.Scan(&acc.Address, &acc.LatestTxId)
		if err != nil {
			return nil, fmt.Errorf("unable to scan solana related account: %w", err)
		}
		res = append(res, acc)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("unable to scan solana related accounts: %w", err)
	}

	return res, nil
}

func devDeleteUserTransactions(
	ctx context.Context,
	pool *pgxpool.Pool,
	userAccountId, walletId int32,
) error {
	const q = "call dev_delete_user_transactions($1, $2)"
	_, err := pool.Exec(ctx, q, userAccountId, walletId)
	if err != nil {
		return fmt.Errorf("unable to call dev_delete_user_transactions: %w", err)
	}
	return nil
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

func enqueueUpdateTxBlockIndex(batch *pgx.Batch, id string, blockIndex int32) *pgx.QueuedQuery {
	const query = `
		update tx set
		data = data || jsonb_build_object('blockIndex', $1::integer)
		where
			id = $2
	`
	return batch.Queue(query, blockIndex, id)
}

func fetchSolanaWallet(
	ctx context.Context,
	pool *pgxpool.Pool,
	rpc *solana.Rpc,
	userAccountId,
	walletId int32,
	walletAddress string,
	latestTxId pgtype.Text,
	fresh bool,
) {
	accounts := make([]solanaAccount, 1)

	if fresh {
		accounts[0] = solanaAccount{
			solanaRelatedAccount{
				Address:    walletAddress,
				LatestTxId: "",
			},
			false,
		}

		err := devDeleteUserTransactions(ctx, pool, userAccountId, walletId)
		assert.NoErr(err, "")
	} else {
		accounts[0] = solanaAccount{
			solanaRelatedAccount{
				Address:    walletAddress,
				LatestTxId: latestTxId.String,
			},
			false,
		}

		relatedAccounts, err := getSolanaRelatedAccounts(ctx, pool, walletId)
		assert.NoErr(err, "unable to get solana related accounts")
		for _, ra := range relatedAccounts {
			accounts = append(accounts, solanaAccount{*ra, true})
		}
	}

	accountsCount := 0
	for len(accounts) > 0 {
		accountsCount += 1

		acc := accounts[0]
		logger.Info(
			"Fetching for account \"%s\" (related: %t latestTxId: %s)",
			acc.Address, acc.related, acc.LatestTxId,
		)

		fetchSolanaAccount(
			ctx,
			pool,
			rpc,
			&accounts,
			userAccountId,
			walletId,
			acc,
		)
		accounts = accounts[1:]
	}

	logger.Info(
		"Txs fetched (accounts %d), starting slots deduplication",
		accountsCount,
	)

	const LIMIT = 100

	for {
		dups, err := getSolanaTxsWithDuplicateSlots(ctx, pool, LIMIT)
		assert.NoErr(err, "")

		slots := make(map[int64][]string)

		for _, tx := range dups {
			signatures, ok := slots[tx.Slot]
			if !ok {
				signatures = make([]string, 0)
			}

			signatures = append(signatures, tx.Id)
			slots[tx.Slot] = signatures
		}

		batch := pgx.Batch{}

		for slot, signatures := range slots {
			res, err := rpc.GetBlockSignatures(uint64(slot), solana.CommitmentConfirmed)
			assert.NoErr(err, "unable to get solana block signatures")
			if res.Error != nil {
				assert.True(
					false,
					"unable to get solana block signatures: %#v",
					*res.Error,
				)
			}

			for idx, s := range res.Result.Signatures {
				exists := slices.Contains(signatures, s)
				if exists {
					q := enqueueUpdateTxBlockIndex(&batch, s, int32(idx))
					q.Exec(func(_ pgconn.CommandTag) error { return nil })
				}
			}
		}

		br := pool.SendBatch(ctx, &batch)
		assert.NoErr(br.Close(), "unable to update tx block indexes")

		if len(dups) < LIMIT {
			break
		}
	}

	logger.Info("Fetching done")
}

func fetchEvmWallet(
	ctx context.Context,
	pool *pgxpool.Pool,
	client *evm.Client,
	userAccountId,
	walletId int32,
	walletAddress string,
	network db.Network,
	walletData *db.EvmWalletData,
) {
	// TODO:  !!!!!!!!! IMPORTANT !!!!!!!!!!
	// fetch the events and internal txs in batches instead of sequentially!!!!!
	chainId, nativeDecimals := evm.ChainIdAndNativeDecimals(network)

	const endBlock = uint64(math.MaxInt64)
	startBlock := walletData.TxsLatestBlockNumber

	feePaid := func(gasUsed, gasPrice *big.Int) decimal.Decimal {
		g := decimal.NewFromBigInt(
			gasUsed,
			-int32(nativeDecimals),
		)
		gp := decimal.NewFromBigInt(gasPrice, 0)
		return g.Mul(gp)
	}

	type evmTx struct {
		data      *db.EvmTransactionData
		timestamp time.Time
		err       bool
	}
	fetchedTxs := make(map[string]*evmTx)
	newWalletData := db.EvmWalletData{}

	for {
		txs, err := client.GetWalletNormalTransactions(
			chainId,
			walletAddress,
			startBlock,
			endBlock,
		)
		assert.NoErr(err, "unable to fetch evm txs")

		if startBlock == walletData.TxsLatestBlockNumber && walletData.TxsLatestHash != "" {
			for i, tx := range txs {
				if tx.Hash == walletData.TxsLatestHash {
					txs = txs[i+1:]
					break
				}
			}
		}

		if len(txs) == 0 {
			break
		}

		logger.Info(
			"Fetched %d txs for %s (%s): from: %d until %d\n",
			len(txs), walletAddress, network, startBlock, endBlock,
		)

		for i, tx := range txs {
			logger.Info("Processing tx %d / %d", i+1, len(txs))

			fee := feePaid((*big.Int)(tx.GasUsed), (*big.Int)(tx.GasPrice))
			value := decimal.NewFromBigInt(
				(*big.Int)(tx.Value),
				-int32(nativeDecimals),
			)

			data := db.EvmTransactionData{
				Block: uint64(tx.BlockNumber),
				TxIdx: int32(tx.TxIdx),
				Fee:   fee,
				Value: value,
				From:  tx.From,
				To:    tx.To,
				Input: db.Uint8Array(tx.Input),
			}

			receipt, err := client.GetTransactionReceipt(network, tx.Hash)
			assert.NoErr(err, "unable to get transaction receipt")

			for _, log := range receipt.Logs {
				var topics [4]db.Uint8Array
				for i, topic := range log.Topics {
					topics[i] = db.Uint8Array(topic)
				}
				data.Events = append(
					data.Events,
					&db.EvmTransactionEvent{
						Address: log.Address,
						Topics:  topics,
						Data:    db.Uint8Array(log.Data),
					},
				)
			}

			internalTxs, err := client.GetInternalTransactionsByHash(
				chainId,
				tx.Hash,
			)
			assert.NoErr(err, "")

			for _, itx := range internalTxs {
				value := decimal.NewFromBigInt(
					(*big.Int)(itx.Value),
					-int32(nativeDecimals),
				)
				data.InternalTxs = append(
					data.InternalTxs,
					&db.EvmInternalTx{
						From:            itx.From,
						To:              itx.To,
						Value:           value,
						ContractAddress: itx.ContractAddress,
						Input:           db.Uint8Array(itx.Input),
					},
				)
			}

			fetchedTxs[tx.Hash] = &evmTx{
				err:       bool(tx.Err),
				timestamp: time.Time(tx.Timestamp),
				data:      &data,
			}

			if i == len(txs)-1 {
				startBlock = uint64(tx.BlockNumber) + 1
				newWalletData.TxsLatestHash = tx.Hash
				newWalletData.TxsLatestBlockNumber = uint64(tx.BlockNumber)
			}
		}
	}

	logger.Info("Fetched %d txs", len(fetchedTxs))

	getTxMetadata := func(
		hash string,
	) (*db.EvmTransactionData, bool) {
		tx, err := client.GetTransactionByHash(network, hash)
		assert.NoErr(err, "unable to get transaction by hash")
		receipt, err := client.GetTransactionReceipt(network, hash)
		assert.NoErr(err, "unable to get transaction receipt")

		fee := feePaid((*big.Int)(receipt.GasUsed), (*big.Int)(tx.GasPrice))
		value := decimal.NewFromBigInt(
			(*big.Int)(tx.Value),
			-int32(nativeDecimals),
		)

		data := &db.EvmTransactionData{
			Block: uint64(tx.BlockNumber),
			TxIdx: int32(tx.TxIdx),
			Input: db.Uint8Array(tx.Input),
			Fee:   fee,
			Value: value,
			From:  tx.From,
			To:    tx.To,
		}

		for _, log := range receipt.Logs {
			var topics [4]db.Uint8Array
			for i, topic := range log.Topics {
				topics[i] = db.Uint8Array(topic)
			}

			data.Events = append(
				data.Events,
				&db.EvmTransactionEvent{
					Address: log.Address,
					Topics:  topics,
					Data:    db.Uint8Array(log.Data),
				},
			)
		}

		internalTxs, err := client.GetInternalTransactionsByHash(
			chainId,
			tx.Hash,
		)
		assert.NoErr(err, "")

		for _, itx := range internalTxs {
			value := decimal.NewFromBigInt(
				(*big.Int)(itx.Value),
				-int32(nativeDecimals),
			)
			data.InternalTxs = append(
				data.InternalTxs,
				&db.EvmInternalTx{
					From:            itx.From,
					To:              itx.To,
					Value:           value,
					ContractAddress: itx.ContractAddress,
					Input:           db.Uint8Array(itx.Input),
				},
			)
		}

		return data, bool(receipt.Err)
	}

	logger.Info("Fetching incoming ERC-20 transfers")
	// NOTE: fetching only events for incoming ERC-20 transfers
	transferTopic, _ := hex.DecodeString("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

	walletBytes, err := hex.DecodeString(walletAddress[2:])
	assert.NoErr(err, fmt.Sprintf("invalid evm walletAddress: %s", walletAddress))

	addressTopic := [32]byte{}
	copy(addressTopic[32-len(walletBytes):], walletBytes)
	topics := [4][]byte{
		transferTopic,
		nil,
		addressTopic[:],
		nil,
	}
	topicsOperators := [6]evm.TopicOperator{}
	topicsOperators[3] = evm.TopicAnd

	startBlock = walletData.EventsLatestBlockNumber

	for {
		events, err := client.GetEventLogsByTopics(
			chainId,
			startBlock,
			endBlock,
			topics,
			topicsOperators,
		)
		assert.NoErr(err, "")

		if startBlock == walletData.EventsLatestBlockNumber && walletData.EventsLatestHash != "" {
			// NOTE: go from back because multiple events can have same tx hash
			for i := len(events) - 1; i >= 0; i-- {
				e := events[i]
				if e.Hash == walletData.EventsLatestHash {
					events = events[i+1:]
					break
				}
			}
		}

		if len(events) == 0 {
			break
		}

		for i, event := range events {
			_, ok := fetchedTxs[event.Hash]

			if !ok {
				logger.Info("Found incoming ERC-20 transfer: %s", event.Hash)
				txData, isErr := getTxMetadata(event.Hash)
				fetchedTxs[event.Hash] = &evmTx{
					err:       isErr,
					timestamp: time.Time(event.Timestamp),
					data:      txData,
				}
			}

			if i == len(events)-1 {
				startBlock = uint64(event.BlockNumber) + 1
				newWalletData.EventsLatestHash = event.Hash
				newWalletData.EventsLatestBlockNumber = uint64(event.BlockNumber)
			}
		}
	}

	/////////////////
	// Fetch internal txs which reference walletAddress, if internal tx is found
	// in a tx which does not reference walletAddress, fetch that tx
	logger.Info("Fetching internal txs")
	startBlock = walletData.InternalTxsLatestBlockNumber

	for {
		internalTxs, err := client.GetInternalTransactionsByAddress(
			chainId,
			walletAddress,
			startBlock,
			endBlock,
		)
		assert.NoErr(err, "")

		if startBlock == walletData.InternalTxsLatestBlockNumber && walletData.InternalTxsLatestHash != "" {
			// NOTE: go from back because multiple can belong to same hash
			for i := len(internalTxs) - 1; i >= 0; i -= 1 {
				t := internalTxs[i]
				if t.Hash == walletData.InternalTxsLatestHash {
					internalTxs = internalTxs[i+1:]
					break
				}
			}
		}

		if len(internalTxs) == 0 {
			break
		}

		logger.Info(
			"Fetched %d internal txs: from %d to %d",
			len(internalTxs), startBlock, endBlock,
		)

		for i, internalTx := range internalTxs {
			_, ok := fetchedTxs[internalTx.Hash]

			if !ok {
				logger.Info("Found internal tx")
				data, isErr := getTxMetadata(internalTx.Hash)
				fetchedTxs[internalTx.Hash] = &evmTx{
					err:       isErr,
					timestamp: time.Time(internalTx.Timestamp),
					data:      data,
				}
			}

			if i == len(internalTxs)-1 {
				startBlock = uint64(internalTx.BlockNumber) + 1
				newWalletData.InternalTxsLatestHash = internalTx.Hash
				newWalletData.InternalTxsLatestBlockNumber = uint64(internalTx.BlockNumber)
			}
		}
	}

	logger.Info("Fetched internal txs")
	batch := pgx.Batch{}
	hashes := make([]string, 0)

	for hash, tx := range fetchedTxs {
		hashes = append(hashes, hash)

		data, err := json.Marshal(tx.data)
		assert.NoErr(err, "unable to marshal tx data")

		q := enqueueInsertTx(
			&batch,
			hash,
			network,
			tx.err,
			tx.data.From,
			tx.data.From,
			tx.timestamp,
			data,
		)
		q.Exec(func(ct pgconn.CommandTag) error { return nil })
	}

	br := pool.SendBatch(ctx, &batch)
	err = br.Close()
	assert.NoErr(err, "unable to insert txs")

	// TODO: should be a tx with the wallet data and some for solana
	err = setUserTransactions(
		ctx,
		pool,
		userAccountId,
		hashes,
		walletId,
		pgtype.Text{},
	)
	assert.NoErr(err, "")

	err = setWalletData(
		ctx,
		pool,
		walletId,
		newWalletData,
	)
	assert.NoErr(err, "")

	// TODO: later can validate balances at the end of the block with
	// eth_getBalance
	// eth_getStorageAt or eth_call ??

	logger.Info("Fetching done")
}

func Fetch(
	ctx context.Context,
	pool *pgxpool.Pool,
	userAccountId int32,
	walletAddress string,
	network string,
	solanaRpc *solana.Rpc,
	etherscanClient *evm.Client,
	fresh bool,
) {
	n, ok := db.NewNetwork(network)
	assert.True(ok, "invalid network: %s", network)

	logger.Info("Starting fetch for user: %d", userAccountId)

	walletId, walletData, err := db.SetWallet(
		ctx,
		pool,
		userAccountId,
		walletAddress,
		n,
	)
	assert.NoErr(err, "")

	// logger.Info(
	// 	"Wallet: %s %s (fresh: %t walletId: %d latestTxId: \"%s\")",
	// 	walletAddress, n, fresh, walletId, latestTxId.String,
	// )

	switch n {
	case db.NetworkSolana:
		wd, ok := walletData.(*db.SolanaWalletData)
		assert.True(ok, "")

		latestTxId := pgtype.Text{}
		if wd.LatestTxId != "" {
			latestTxId.Valid = true
			latestTxId.String = wd.LatestTxId
		}

		fetchSolanaWallet(
			ctx,
			pool,
			solanaRpc,
			userAccountId,
			walletId,
			walletAddress,
			latestTxId,
			fresh,
		)
	case db.NetworkArbitrum, db.NetworkAvaxC, db.NetworkBsc, db.NetworkEthereum:
		wd, ok := walletData.(*db.EvmWalletData)
		assert.True(ok, "")

		fetchEvmWallet(
			ctx,
			pool,
			etherscanClient,
			userAccountId,
			walletId,
			walletAddress,
			n,
			wd,
		)
	}
}
