package solana

import (
	"encoding/binary"
	"taxee/pkg/db"
)

const ASSOCIATED_TOKEN_PROGRAM_ADDRESS = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"

type associatedTokenIx int

const (
	associatedTokenIxCreate associatedTokenIx = iota
	associatedTokenIxCreateIdempotent
	associatedTokenIxRecoverNested
)

func associatedTokenIxFromData(data []byte) (ix associatedTokenIx, ok bool) {
	ok = true

	if len(data) == 0 {
		ix = associatedTokenIxCreate
		return
	}

	switch data[0] {
	case 0:
		ix = associatedTokenIxCreate
	case 1:
		ix = associatedTokenIxCreateIdempotent
	case 2:
		ix = associatedTokenIxRecoverNested
	default:
		ok = false
		return
	}

	return
}

func associatedTokenIxRelatedAccounts(
	relatedAccounts relatedAccounts,
	walletAddress string,
	ix *db.SolanaInstruction,
) {
	ixType, ok := associatedTokenIxFromData(ix.Data)
	if !ok {
		return
	}

	switch ixType {
	case associatedTokenIxCreate, associatedTokenIxCreateIdempotent:
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

func preprocessAssociatedTokenIx(ctx *Context, ix *db.SolanaInstruction) {
	ixType, ok := associatedTokenIxFromData(ix.Data)
	if !ok {
		return
	}

	switch ixType {
	case associatedTokenIxCreateIdempotent:
		if len(ix.InnerInstructions) == 0 {
			return
		}
		fallthrough
	case associatedTokenIxCreate:
		tokenAccount, owner, mint := ix.Accounts[1], ix.Accounts[2], ix.Accounts[3]

		if !ctx.walletOwned(owner) {
			return
		}

		// inner ixs
		// https://github.com/solana-program/associated-token-account/blob/main/program/src/tools/account.rs#L19
		//
		// len 5 (account has enough lamports) -> transfer 2nd
		// len 6 (account has lamports but not enough) -> transfer 2nd
		// len 4 (account empty) -> transfer 2nd
		transferIx := ix.InnerInstructions[1]
		amount := binary.LittleEndian.Uint64(transferIx.Data[4:])
		ctx.accountReceiveSol(tokenAccount, amount)

		data := TokenAccountData{
			Mint:  mint,
			Owner: owner,
		}
		ctx.accountInitOwned(tokenAccount, &data)
	case associatedTokenIxRecoverNested:
	}
}
