package fetcher

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"taxee/cmd/fetcher/solana"
	solanaparsing "taxee/cmd/parser/solana"
	"taxee/pkg/assert"
	"taxee/pkg/db"
	"taxee/pkg/logger"
	"time"
	"unsafe"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func setWallet(
	ctx context.Context,
	pool *pgxpool.Pool,
	userAccountId int32,
	walletAddress string,
	network db.Network,
) (walletId int32, latestTxId sql.NullString, err error) {
	const query = "select wallet_id, wallet_latest_tx_id from set_wallet($1, $2, $3)"
	row := pool.QueryRow(ctx, query, userAccountId, walletAddress, network)

	scanErr := row.Scan(&walletId, &latestTxId)
	if scanErr != nil {
		err = fmt.Errorf("unable to scan wallet: %w", scanErr)
	}

	return
}

func enqueueInsertTx(
	batch *pgx.Batch,
	txId string,
	err bool,
	fee uint64,
	signer, feePayer string,
	timestamp time.Time,
	txDataMarshalled []byte,
) {
	const query = `
		insert into tx (
			id, network, err, fee, signer, fee_payer, timestamp, data
		) values (
			$1, $2, $3, $4, $5, $6, $7, $8
		) on conflict (id) do nothing
	`
	batch.Queue(
		query,
		txId,
		db.NetworkSolana,
		err,
		fee,
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
	relatedAccountAddress sql.NullString,
) error {
	query := "call set_user_transactions($1, $2, $3, $4)"
	_, err := pool.Exec(ctx, query, userAccountId, txIds, walletId, relatedAccountAddress)
	if err != nil {
		return fmt.Errorf("unable to query set_user_transactions: %w", err)
	}
	return nil
}

func setLatestWalletTxId(
	ctx context.Context,
	pool *pgxpool.Pool,
	walletId int32,
	latestTxId string,
) error {
	const query = `
		update wallet set
			latest_tx_id = $1
		where id = $2
	`
	_, err := pool.Exec(ctx, query, latestTxId, walletId)
	if err != nil {
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

func fetchSolana(
	ctx context.Context,
	pool *pgxpool.Pool,
	rpc *solana.Rpc,
	accounts *[]solanaAccount,
	userAccountId,
	walletId int32,
	account solanaAccount,
) {
	logger.Info("Starting fetching transactions for address: %s", account.Address)
	relatedAccountAddress := sql.NullString{
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

					txData := db.SolanaTransactionData{
						Slot:           tx.Slot,
						Instructions:   ixs,
						NativeBalances: *(*nativeBalances)(unsafe.Pointer(&tx.NativeBalances)),
						TokenBalances:  *(*tokenBalances)(unsafe.Pointer(&tx.TokenBalances)),
						TokenDecimals:  tx.TokenDecimals,
						BlockIndex:     -1,
					}

					if !account.related && !tx.Err {
						solanaparsing.RelatedAccountsFromTx(
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
						tx.Err,
						tx.Fee,
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
		err = setLatestWalletTxId(ctx, pool, walletId, newLastestTxId)
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

func Fetch(
	ctx context.Context,
	pool *pgxpool.Pool,
	userAccountId int32,
	walletAddress string,
	network string,
	rpc *solana.Rpc,
	fresh bool,
) {
	n, ok := db.NewNetwork(network)
	assert.True(ok, "invalid network: %s", network)

	logger.Info("Starting fetch for user: %d", userAccountId)

	walletId, latestTxId, err := setWallet(ctx, pool, userAccountId, walletAddress, n)
	assert.NoErr(err, "")
	logger.Info(
		"Wallet: %s %s (fresh: %t walletId: %d latestTxId: \"%s\")",
		walletAddress, n, fresh, walletId, latestTxId.String,
	)

	switch n {
	case db.NetworkSolana:
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

			fetchSolana(
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
}
