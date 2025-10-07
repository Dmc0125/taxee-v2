package parser

import (
	"taxee/pkg/assert"
	"taxee/pkg/db"

	"github.com/shopspring/decimal"
)

type inventoryAccountId struct {
	network db.Network
	address string
	token   string
}

type inventoryAccount struct {
	amount   decimal.Decimal
	acqPrice decimal.Decimal
}

type inventory struct {
	acquisition decimal.Decimal
	disposal    decimal.Decimal
	income      decimal.Decimal
	accounts    map[inventoryAccountId][]*inventoryAccount
}

func (inv *inventory) processEvent(event *db.Event) {
	internalBalances := make([]*inventoryAccount, 0)

	for _, transfer := range event.Transfers {
		accountId := inventoryAccountId{event.Network, transfer.Account, transfer.Token}
		accountBalances := inv.accounts[accountId]

		internal := transfer.Type>>7 == 1

		switch transfer.Type & db.EventTransferTypeMask {
		case db.EventTransferOutgoing:
			remainingAmount := transfer.Amount

			for len(accountBalances) > 0 {
				balance := accountBalances[0]

				if remainingAmount.GreaterThan(balance.amount) {
					if internal {
						internalBalances = append(
							internalBalances,
							accountBalances[0],
						)
					} else {
						dispValue := balance.amount.Mul(transfer.Price)
						transfer.Profit = transfer.Profit.Add(
							dispValue.Sub(
								balance.amount.Sub(balance.acqPrice),
							),
						)
						inv.disposal = inv.disposal.Add(dispValue)
					}

					remainingAmount = remainingAmount.Sub(balance.amount)
					accountBalances = accountBalances[1:]
				} else {
					if internal {
						internalBalances = append(
							internalBalances,
							accountBalances[0],
						)
					} else {
						dispValue := remainingAmount.Mul(transfer.Price)
						transfer.Profit = transfer.Profit.Add(
							dispValue.Sub(
								remainingAmount.Sub(balance.acqPrice),
							),
						)
						inv.disposal = inv.disposal.Add(dispValue)
					}

					balance.amount = balance.amount.Sub(remainingAmount)
					remainingAmount = decimal.Zero
				}
			}

			if remainingAmount.GreaterThan(decimal.Zero) {
				// TODO: missing balance error
				if internal {
					internalBalances = append(internalBalances, &inventoryAccount{
						amount:   remainingAmount,
						acqPrice: decimal.Zero,
					})
				} else {
					dispValue := remainingAmount.Mul(transfer.Price)
					inv.income = inv.income.Add(dispValue)
					transfer.Profit = transfer.Profit.Add(dispValue)
				}
			}
		case db.EventTransferIncoming:
			if !internal {
				acqValue := transfer.Amount.Mul(transfer.Price)
				inv.income = inv.income.Add(acqValue)
				transfer.Profit = acqValue
				accountBalances = append(accountBalances, &inventoryAccount{
					amount:   transfer.Amount,
					acqPrice: transfer.Price,
				})
			} else {
				for _, b := range internalBalances {
					accountBalances = append(accountBalances, b)
				}
			}
		default:
			assert.True(false, "unknown event type: %s", transfer.Type)
		}

		inv.accounts[accountId] = accountBalances

	}
}
