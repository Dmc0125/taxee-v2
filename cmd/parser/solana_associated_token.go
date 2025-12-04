package parser

import (
	"encoding/binary"
	"taxee/pkg/db"
)

const SOL_ASSOCIATED_TOKEN_PROGRAM_ADDRESS = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"

type solAssociatedTokenIx int

const (
	solAssociatedTokenIxCreate solAssociatedTokenIx = iota
	solAssociatedTokenIxCreateIdempotent
	solAssociatedTokenIxRecoverNested
)

func solAssociatedTokenIxFromData(data []byte) (ix solAssociatedTokenIx, ok bool) {
	ok = true

	if len(data) == 0 {
		ix = solAssociatedTokenIxCreate
		return
	}

	switch data[0] {
	case 0:
		ix = solAssociatedTokenIxCreate
	case 1:
		ix = solAssociatedTokenIxCreateIdempotent
	case 2:
		ix = solAssociatedTokenIxRecoverNested
	default:
		ok = false
		return
	}

	return
}

func solAssociatedTokenIxRelatedAccounts(
	relatedAccounts relatedAccounts,
	walletAddress string,
	ix *db.SolanaInstruction,
) {
	ixType, ok := solAssociatedTokenIxFromData(ix.Data)
	if !ok {
		return
	}

	switch ixType {
	case solAssociatedTokenIxCreate, solAssociatedTokenIxCreateIdempotent:
		if len(ix.InnerInstructions) == 0 {
			return
		}

		owner := ix.Accounts[2]
		if walletAddress == owner {
			relatedAccounts.Append(ix.Accounts[1])
			return
		}
	}
}

func solPreprocessAssociatedTokenIx(ctx *solContext, ix *db.SolanaInstruction) {
	ixType, ok := solAssociatedTokenIxFromData(ix.Data)
	if !ok {
		return
	}

	switch ixType {
	case solAssociatedTokenIxCreateIdempotent:
		if len(ix.InnerInstructions) == 0 {
			return
		}
		fallthrough
	case solAssociatedTokenIxCreate:
		tokenAccount, owner, mint := ix.Accounts[1], ix.Accounts[2], ix.Accounts[3]

		if !ctx.walletOwned(owner) {
			return
		}

		// TODO: this is wrong

		// inner ixs
		// https://github.com/solana-program/associated-token-account/blob/main/program/src/tools/account.rs#L19
		//
		// len 5 (account has enough lamports) -> transfer 2nd
		// len 6 (account has lamports but not enough) -> transfer 2nd
		// len 4 (account empty) -> transfer 2nd
		transferIx := ix.InnerInstructions[1]
		amount := binary.LittleEndian.Uint64(transferIx.Data[4:])
		ctx.receiveSol(tokenAccount, amount)

		data := solTokenAccountData{
			Mint:  mint,
			Owner: owner,
		}
		ctx.init(tokenAccount, true, &data)
	case solAssociatedTokenIxRecoverNested:
	}
}

func solProcessAssociatedTokenIx(
	ctx *solContext,
	ix *db.SolanaInstruction,
) {
	ixType, ok := solAssociatedTokenIxFromData(ix.Data)
	if !ok {
		return
	}

	if ixType > solAssociatedTokenIxCreateIdempotent {
		return
	}
	if len(ix.InnerInstructions) == 0 {
		return
	}

	var (
		payer        = ix.Accounts[0]
		tokenAccount = ix.Accounts[1]
		owner        = ix.Accounts[2]
	)

	fromInternal, toInternal := ctx.walletOwned(payer), ctx.walletOwned(owner)

	if !fromInternal && !toInternal {
		return
	}

	transferIx := ix.InnerInstructions[1]
	amount := binary.LittleEndian.Uint64(transferIx.Data[4:])

	event := solNewEvent(ctx, "associated_token", "create", db.EventTypeTransfer)
	event.Transfers = append(event.Transfers, &db.EventTransfer{
		Direction:   getTransferEventDirection(fromInternal, toInternal),
		FromWallet:  payer,
		ToWallet:    owner,
		FromAccount: payer,
		ToAccount:   tokenAccount,
		Token:       SOL_MINT_ADDRESS,
		Amount:      newDecimalFromRawAmount(amount, 9),
		TokenSource: uint16(db.NetworkSolana),
	})
}
