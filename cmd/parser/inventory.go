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

func invSubBalance(
	balances *[]*inventoryAccount,
	amount decimal.Decimal,
) (newAmount decimal.Decimal, removedAmount decimal.Decimal) {
	balance := (*balances)[0]

	if amount.GreaterThanOrEqual(balance.amount) {
		removedAmount = balance.amount
		newAmount = amount.Sub(balance.amount)
		*balances = (*balances)[1:]
	} else {
		removedAmount = amount
		balance.amount = balance.amount.Sub(amount)
		newAmount = decimal.Zero
	}

	return
}

func (inv *inventory) processEvent(event *db.Event) {
	switch data := event.Data.(type) {
	case *db.EventTransferInternal:
		fromAccountId := inventoryAccountId{event.Network, data.FromAccount, data.Token}
		fromBalances := inv.accounts[fromAccountId]

		toAccountId := inventoryAccountId{event.Network, data.ToAccount, data.Token}
		toBalances := inv.accounts[toAccountId]

		remainingAmount := data.Amount

		for len(fromBalances) > 0 && remainingAmount.GreaterThan(decimal.Zero) {
			acqPrice := fromBalances[0].acqPrice

			var movedAmount decimal.Decimal
			remainingAmount, movedAmount = invSubBalance(
				&fromBalances,
				remainingAmount,
			)

			toBalances = append(toBalances, &inventoryAccount{
				amount:   movedAmount,
				acqPrice: acqPrice,
			})
		}

		if remainingAmount.GreaterThan(decimal.Zero) {
			// TODO: missing amount error
			inv.income = inv.income.Add(data.Value)

			toBalances = append(toBalances, &inventoryAccount{
				amount:   remainingAmount,
				acqPrice: data.Price,
			})
		}

		inv.accounts[fromAccountId] = fromBalances
		inv.accounts[toAccountId] = toBalances
	case *db.EventTransfer:
		accountId := inventoryAccountId{event.Network, data.Account, data.Token}
		balances := inv.accounts[accountId]

		switch data.Direction {
		case db.EventTransferIncoming:
			if event.Type == db.EventTypeTransfer {
				inv.income = inv.income.Add(data.Value)
				data.Profit = data.Value
			}

			balances = append(balances, &inventoryAccount{
				amount:   data.Amount,
				acqPrice: data.Price,
			})
		case db.EventTransferOutgoing:
			remainingAmount := data.Amount

			for len(balances) > 0 && remainingAmount.GreaterThan(decimal.Zero) {
				acqPrice := balances[0].acqPrice

				var movedAmount decimal.Decimal
				remainingAmount, movedAmount = invSubBalance(
					&balances,
					remainingAmount,
				)

				// TODO: should burn increase profits?
				if event.Type == db.EventTypeTransfer {
					disposal := movedAmount.Mul(data.Price)
					acquisition := movedAmount.Mul(acqPrice)

					data.Profit = disposal.Sub(acquisition)
					inv.disposal = inv.disposal.Add(disposal)
					inv.acquisition = inv.acquisition.Add(acquisition)
				}
			}
		default:
			assert.True(false, "invalid direction: %d", data.Direction)
		}

		inv.accounts[accountId] = balances
	}
}
