package fetcher

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
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
	rpc *solana.Rpc,
	accounts *[]solanaAccount,
	userAccountId,
	walletId int32,
	account solanaAccount,
) error {
	logger.Info("Starting fetching transactions for address: %s", account.Address)
	relatedAccountAddress := pgtype.Text{
		Valid:  account.related,
		String: account.Address,
	}

	const LIMIT = 1000
	before, newLastestTxId := "", ""
	retry := make([]string, 0)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

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
		if err != nil {
			return err
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

		const BATCH_SIZE = 100
		insertTransactionsBatch := pgx.Batch{}
		txIds := make([]string, 0)

		logger.Info("Txs chunks %d", len(signaturesResponse.Result)/BATCH_SIZE+1)
		chunkIdx := 0

		for chunk := range slices.Chunk(signaturesResponse.Result, BATCH_SIZE) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			chunkIdx += 1
			batch := make([]*solana.GetMultipleTransactionsParams, len(chunk))
			for i, sig := range chunk {
				batch[i] = &solana.GetMultipleTransactionsParams{
					Signature:  sig.Signature,
					Commitment: solana.CommitmentConfirmed,
				}
			}

			batchRes, err := rpc.GetMultipleTransactions(batch)
			if err != nil {
				return err
			}

			logger.Info("Fetched txs chunk %d", chunkIdx)

			for i, txResponse := range batchRes {
				switch {
				case txResponse.Error != nil:
					// TODO: Find out what is an error that occurs on too many
					// requests
					logger.Error("Tx response err: %#v", *txResponse.Error)
					retry = append(retry, batch[i].Signature)
				default:
					// TODO: is it needed to validate the response? probably yes
					compiledTx, err := solana.ParseTransaction(txResponse.Result.Transaction[0])
					if err != nil {
						return fmt.Errorf("unbale to parse solana transaction: %w", err)
					}
					tx, err := solana.DecompileTransaction(
						txResponse.Result.Slot,
						txResponse.Result.BlockTime,
						txResponse.Result.Meta,
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

					insertTransactionsBatch.Queue(
						insertTransactionQuery,
						tx.Signature, db.NetworkSolana, tx.Err,
						tx.Accounts[0], tx.Accounts[0], tx.Blocktime,
						marshalled,
					)
					txIds = append(txIds, tx.Signature)
				}
			}
		}

		if insertTransactionsBatch.Len() > 0 {
			br := pool.SendBatch(ctx, &insertTransactionsBatch)
			if err := br.Close(); err != nil {
				return fmt.Errorf("unable to insert transactions batch: %w", err)
			}

			if relatedAccountAddress.Valid {
				_, err := pool.Exec(
					ctx, "call set_transactions_for_related_account($1, $2, $3, $4)",
					userAccountId, txIds, walletId, relatedAccountAddress,
				)
				if err != nil {
					return fmt.Errorf("unable to transactions for related account: %w", err)
				}
			} else {
				_, err := pool.Exec(
					ctx, setTransactionsForWallet,
					userAccountId, txIds, walletId,
				)
				if err != nil {
					return fmt.Errorf("unable to transactions for wallet: %w", err)
				}
			}
		}

		if len(signaturesResponse.Result) < LIMIT && len(retry) == 0 {
			break
		}
	}

	if newLastestTxId == "" {
		return nil
	}

	if relatedAccountAddress.Valid {
		_, err := pool.Exec(
			ctx, setLatestSolanaRelatedAccountTxId,
			// args,
			newLastestTxId, walletId, relatedAccountAddress.String,
		)
		if err != nil {
			return fmt.Errorf("unable to set latest tx id for solana related account: %w", err)
		}

		logger.Info(
			"Set latest txId \"%s\" for related account %s",
			newLastestTxId,
			relatedAccountAddress.String,
		)
	} else {
		walletData, err := json.Marshal(db.SolanaWalletData{LatestTxId: newLastestTxId})
		assert.NoErr(err, "")

		_, err = pool.Exec(ctx, setWalletDataQuery, walletData, walletId)
		if err != nil {
			return fmt.Errorf("unable to set latest tx id for solana wallet: %w", err)
		}

		logger.Info(
			"Set latest txId \"%s\" for wallet %s",
			newLastestTxId,
			account.Address,
		)
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

func fetchSolanaWallet(
	ctx context.Context,
	pool *pgxpool.Pool,
	rpc *solana.Rpc,
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
		logger.Info(
			"Fetching for account \"%s\" (related: %t latestTxId: %s)",
			acc.Address, acc.related, acc.LatestTxId,
		)

		err := fetchSolanaAccount(
			ctx,
			pool,
			rpc,
			&accounts,
			userAccountId,
			walletId,
			acc,
		)
		if err != nil {
			return err
		}
		accounts = accounts[1:]
	}

	logger.Info(
		"Txs fetched (accounts %d), starting slots deduplication",
		accountsCount,
	)

	const LIMIT = 100

	for {
		dups, err := getSolanaTxsWithDuplicateSlots(ctx, pool, LIMIT)
		if err != nil {
			return fmt.Errorf("unable to get solana txs with duplicate slots: %w", err)
		}

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
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			res, err := rpc.GetBlockSignatures(uint64(slot), solana.CommitmentConfirmed)
			if err != nil {
				return err
			}

			for idx, s := range res.Result.Signatures {
				exists := slices.Contains(signatures, s)
				if exists {
					const query = `
						update tx set
						data = data || jsonb_build_object('blockIndex', $1::integer)
						where
							id = $2
					`
					batch.Queue(query, int32(idx), s)
				}
			}
		}

		br := pool.SendBatch(ctx, &batch)
		if err := br.Close(); err != nil {
			return fmt.Errorf("unable to update solana txs with duplicate slots: %w", err)
		}

		if len(dups) < LIMIT {
			break
		}
	}

	logger.Info("Fetching done")
	return nil
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
) error {
	chainId, nativeDecimals, err := evm.ChainIdAndNativeDecimals(network)
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
	fetchedTxs := make(map[string]*evmTx)

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

		logger.Info("fetching normal txs from block: %d", txsStartBlock)
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

			fetchedTxs[tx.Hash] = &evmTx{
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

		logger.Info("fetching internal txs from block: %d", internalTxsStartBlock)
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
			if _, ok := fetchedTxs[tx.Hash]; !ok {
				fetchedTxs[tx.Hash] = &evmTx{
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

		logger.Info("fetching incoming transfer from block: %d", eventsStartBlock)
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
			if _, ok := fetchedTxs[event.Hash]; !ok {
				fetchedTxs[event.Hash] = &evmTx{
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
		logger.Info("fetching done - no new txs found")
		return nil
	}

	type messageInternalTxs struct {
		hash        string
		internalTxs []*db.EvmInternalTx
	}
	chanInternalTxs := make(chan any)

	var batchTxsByHash []string
	var batchReceipts []string

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

			internalTxs, err := client.GetInternalTransactionsByHash(chainId, hash)
			if err != nil {
				chanInternalTxs <- fmt.Errorf("unable to get internal tx by hash: %w", err)
				close(chanInternalTxs)
				return
			}

			m := messageInternalTxs{
				hash: hash,
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

	for hash, tx := range fetchedTxs {
		if tx.fetchNormalTx {
			batchTxsByHash = append(batchTxsByHash, hash)
		}
		batchReceipts = append(batchReceipts, hash)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if len(batchTxsByHash) > 0 {
		logger.Info("fetching %d remaining normal txs", len(batchTxsByHash))
		txsResponse, err := client.BatchGetTransactionByHash(network, batchTxsByHash)
		if err != nil {
			return fmt.Errorf("unable to get transactions by hashes: %w", err)
		}

		for _, txResponse := range txsResponse {
			if txResponse.Error != nil {
				return fmt.Errorf("unable to get transactions by hashes: %s", txResponse.Error.Message)
			}
			tx := txResponse.Result

			value := decimal.NewFromBigInt((*big.Int)(tx.Value), -int32(nativeDecimals))

			fetchedTxs[tx.Hash].data = &db.EvmTransactionData{
				Block: uint64(tx.BlockNumber),
				TxIdx: int32(tx.TxIdx),
				Input: db.Uint8Array(tx.Input),
				Value: value,
				From:  tx.From,
				To:    tx.To,
			}
			fetchedTxs[tx.Hash].gasPrice = (*big.Int)(tx.GasPrice)
		}
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if len(batchReceipts) > 0 {
		logger.Info("fetching %d receipts", len(batchReceipts))
		receiptsResponse, err := client.BatchGetTransactionReceipt(network, batchReceipts)
		if err != nil {
			return fmt.Errorf("unable to get transactios receipts: %w", err)
		}

		for _, receiptResponse := range receiptsResponse {
			if receiptResponse.Error != nil {
				return fmt.Errorf("unable to get transactions receipts: %s", receiptResponse.Error.Message)
			}

			receipt := receiptResponse.Result
			tx := fetchedTxs[receipt.Hash]

			tx.err = bool(receipt.Err)
			if tx.gasPrice != nil {
				tx.data.Fee = calcFeePaid((*big.Int)(receipt.GasUsed), tx.gasPrice)
			}

			for _, log := range receipt.Logs {
				var topics [4]db.Uint8Array
				for i, topic := range log.Topics {
					topics[i] = db.Uint8Array(topic)
				}

				tx.data.Events = append(
					tx.data.Events,
					&db.EvmTransactionEvent{
						Address: log.Address,
						Topics:  topics,
						Data:    db.Uint8Array(log.Data),
					},
				)
			}
		}
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
			logger.Info("fetched internal transaction %d/%d", i, len(fetchedTxs))
			tx := fetchedTxs[m.hash]
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

			// TODO: error is not being set ?
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
	logger.Info("Fetching done")
	return err
}

func Fetch(
	ctx context.Context,
	pool *pgxpool.Pool,
	solanaRpc *solana.Rpc,
	etherscanClient *evm.Client,
	//
	userAccountId int32,
	network db.Network,
	walletAddress string,
	walletId int32,
	walletDataSerialized json.RawMessage,
	fresh bool,
) error {
	logger.Info(
		"Starting fetch: ",
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

		return fetchSolanaWallet(
			ctx,
			pool,
			solanaRpc,
			userAccountId,
			walletId,
			walletAddress,
			latestTxId,
			fresh,
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
			ctx,
			pool,
			etherscanClient,
			userAccountId,
			walletId,
			walletAddress,
			network,
			&walletData,
		)
	default:
		return fmt.Errorf("%w: invalid network", ERROR_INVALID_DATA)
	}
}
