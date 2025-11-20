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

func invSubFromAccount(
	inv *inventory,
	account, token string,
	tokenSource uint16,
	event *db.Event,
	amount, price decimal.Decimal,
	profit *decimal.Decimal,
	increaseProfits bool,
	missingBalancesErrorsContainer *errorsContainer,
) {
	accountId := inventoryAccountId{event.Network, account, token}
	balances := inv.accounts[accountId]

	remainingAmount := amount

	for len(balances) > 0 && remainingAmount.GreaterThan(decimal.Zero) {
		acqPrice := balances[0].acqPrice

		var movedAmount decimal.Decimal
		remainingAmount, movedAmount = invSubBalance(
			&balances,
			remainingAmount,
		)

		if increaseProfits {
			disposal := movedAmount.Mul(price)
			acquisition := movedAmount.Mul(acqPrice)

			*profit = disposal.Sub(acquisition)
			inv.disposal = inv.disposal.Add(disposal)
			inv.acquisition = inv.acquisition.Add(acquisition)
		}
	}

	if remainingAmount.GreaterThan(decimal.Zero) {
		err := db.ParserError{
			TxId:     event.TxId,
			IxIdx:    event.IxIdx,
			EventIdx: int32(event.Idx),
			Type:     db.ParserErrorTypeMissingBalance,
			Data: &db.ParserErrorMissingBalance{
				AccountAddress: account,
				Token:          token,
				Amount:         remainingAmount,
				TokenSource:    tokenSource,
			},
		}
		appendParserError(
			&missingBalancesErrorsContainer.count,
			&missingBalancesErrorsContainer.errors,
			&err,
		)

		if increaseProfits {
			value := remainingAmount.Mul(price)
			*profit = value
			inv.income = inv.income.Add(value)
		}
	}

	inv.accounts[accountId] = balances
}

func (inv *inventory) processEvent(
	event *db.Event,
	missingBalancesErrorsContainer *errorsContainer,
) {
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
			err := db.ParserError{
				TxId:     event.TxId,
				IxIdx:    event.IxIdx,
				EventIdx: int32(event.Idx),
				Type:     db.ParserErrorTypeMissingBalance,
				Data: &db.ParserErrorMissingBalance{
					AccountAddress: data.FromAccount,
					Token:          data.Token,
					Amount:         remainingAmount,
					TokenSource:    data.TokenSource,
				},
			}
			appendParserError(
				&missingBalancesErrorsContainer.count,
				&missingBalancesErrorsContainer.errors,
				&err,
			)

			value := remainingAmount.Mul(data.Price)
			data.Profit = value
			inv.income = inv.income.Add(value)

			toBalances = append(toBalances, &inventoryAccount{
				amount:   remainingAmount,
				acqPrice: data.Price,
			})
		}

		inv.accounts[fromAccountId] = fromBalances
		inv.accounts[toAccountId] = toBalances
	case *db.EventTransfer:
		switch data.Direction {
		case db.EventTransferIncoming:
			if event.Type == db.EventTypeTransfer {
				inv.income = inv.income.Add(data.Value)
				data.Profit = data.Value
			}

			accountId := inventoryAccountId{event.Network, data.Account, data.Token}
			inv.accounts[accountId] = append(
				inv.accounts[accountId],
				&inventoryAccount{
					amount:   data.Amount,
					acqPrice: data.Price,
				},
			)
		case db.EventTransferOutgoing:
			invSubFromAccount(
				inv,
				data.Account, data.Token, data.TokenSource,
				event,
				data.Amount, data.Price, &data.Profit,
				event.Type == db.EventTypeTransfer,
				missingBalancesErrorsContainer,
			)
		default:
			assert.True(false, "invalid direction: %d", data.Direction)
		}

	case *db.EventSwap:
		for _, transfer := range data.Outgoing {
			invSubFromAccount(
				inv,
				transfer.Account, transfer.Token, transfer.TokenSource,
				event,
				transfer.Amount, transfer.Price, &transfer.Profit,
				true,
				missingBalancesErrorsContainer,
			)
		}

		for _, transfer := range data.Incoming {
			inv.income = inv.income.Add(transfer.Value)
			transfer.Profit = transfer.Value

			accountId := inventoryAccountId{event.Network, transfer.Account, transfer.Token}
			inv.accounts[accountId] = append(inv.accounts[accountId], &inventoryAccount{
				amount:   transfer.Amount,
				acqPrice: transfer.Price,
			})
		}
	}
}
