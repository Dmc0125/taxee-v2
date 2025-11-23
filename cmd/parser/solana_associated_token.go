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

func solProcessAssociatedTokenIx(
	ctx *solContext,
	ix *db.SolanaInstruction,
	events *[]*db.Event,
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
		payerAddress        = ix.Accounts[0]
		tokenAccountAddress = ix.Accounts[1]
		ownerAddress        = ix.Accounts[2]
	)

	fromInternal, toInternal := ctx.walletOwned(payerAddress), ctx.walletOwned(ownerAddress)

	if !fromInternal && !toInternal {
		return
	}

	transferIx := ix.InnerInstructions[1]
	amount := binary.LittleEndian.Uint64(transferIx.Data[4:])
	var toAccountBalance uint64

	// NOTE: from is always native account, so only care about to
	if toInternal {
		tokenAccount := ctx.accounts[tokenAccountAddress]
		if tokenAccount == nil {
			tokenAccount = &solAccount{}
		}
		toAccountBalance = tokenAccount.balance
		tokenAccount.balance += amount
	}

	event := solNewEvent(ctx)
	event.UiAppName = "associated_token_program"
	event.UiMethodName = "create"

	// TODO: every system / SOL transfer needs to update the account balances
	setEventTransfer(
		event,
		payerAddress, ownerAddress,
		payerAddress, tokenAccountAddress,
		fromInternal, toInternal,
		0, toAccountBalance,
		newDecimalFromRawAmount(amount, 9),
		SOL_MINT_ADDRESS,
		uint16(db.NetworkSolana),
	)
	*events = append(*events, event)

	for i := len(*events) - 1; i >= 0; i -= 1 {
		e := (*events)[i]

		t, ok := e.Data.(*db.EventTransfer)
		if !ok {
			continue
		}
		transfer := *t

		switch transfer.Direction {
		case db.EventTransferIncoming:
			// NOTE: incoming transfers do not exist, since that account
			// is unknown
		case db.EventTransferOutgoing:
			if t.OtherAccount == tokenAccountAddress {
				e.Type = db.EventTypeTransferInternal
				e.Data = &db.EventTransferInternal{
					FromWallet:  t.OwnedWallet,
					ToWallet:    t.OtherWallet,
					FromAccount: t.OwnedWallet,
					ToAccount:   t.OtherAccount,
					Token:       t.Token,
					Amount:      t.Amount,
					TokenSource: t.TokenSource,
				}

				if t.OtherNativeBalance == 0 {
					break
				}
			}
		}
	}
}
