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
	errors *[]*db.ParserError,
) decimal.Decimal {
	accountId := inventoryAccountId{event.Network, account, token}
	balances := inv.accounts[accountId]

	remainingAmount := amount
	value := decimal.Zero

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

			value = value.Add(disposal)
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
		appendParserError(errors, &err)

		if increaseProfits {
			remainingValue := remainingAmount.Mul(price)
			*profit = remainingValue
			inv.income = inv.income.Add(remainingValue)

			value = value.Add(remainingValue)
		}
	}

	inv.accounts[accountId] = balances

	return value
}

func (inv *inventory) processEvent(
	event *db.Event,
	errors *[]*db.ParserError,
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
			appendParserError(errors, &err)

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

			accountId := inventoryAccountId{event.Network, data.OwnedAccount, data.Token}
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
				data.OwnedAccount, data.Token, data.TokenSource,
				event,
				data.Amount, data.Price, &data.Profit,
				event.Type == db.EventTypeTransfer,
				errors,
			)
		default:
			assert.True(false, "invalid direction: %d", data.Direction)
		}
	case *db.EventSwap:
		type queued struct {
			incoming  bool
			transfers []*db.EventSwapTransfer
		}
		queue := [2]queued{}

		switch event.Type {
		case db.EventTypeRemoveLiquidity:
			assert.True(len(data.Outgoing) == 1, "multiple LP tokens in remove_liquidity")
			queue[0] = queued{
				incoming:  true,
				transfers: data.Incoming,
			}
			queue[1] = queued{
				incoming:  false,
				transfers: data.Outgoing,
			}
		case db.EventTypeAddLiquidity:
			assert.True(len(data.Incoming) == 1, "multiple LP tokens in add_liquidity")
			fallthrough
		default:
			queue[0] = queued{
				incoming:  false,
				transfers: data.Outgoing,
			}
			queue[1] = queued{
				incoming:  true,
				transfers: data.Incoming,
			}
		}

		tokensValue := decimal.Zero

		for _, q := range queue {
			if q.incoming {
				for _, t := range q.transfers {
					if event.Type == db.EventTypeAddLiquidity && t.Value.IsZero() {
						t.Value = tokensValue
						t.Price = tokensValue.Div(t.Amount)
					} else if event.Type == db.EventTypeRemoveLiquidity {
						tokensValue = tokensValue.Add(t.Value)
					}

					accountId := inventoryAccountId{event.Network, t.Account, t.Token}
					inv.accounts[accountId] = append(inv.accounts[accountId], &inventoryAccount{
						amount:   t.Amount,
						acqPrice: t.Price,
					})
				}
			} else {
				for _, t := range q.transfers {
					if event.Type == db.EventTypeRemoveLiquidity && t.Value.IsZero() {
						t.Value = tokensValue
						t.Price = tokensValue.Div(t.Amount)
					}

					value := invSubFromAccount(
						inv,
						t.Account, t.Token, t.TokenSource,
						event,
						t.Amount, t.Price, &t.Profit,
						true,
						errors,
					)
					if event.Type == db.EventTypeAddLiquidity {
						tokensValue = tokensValue.Add(value)
					}
				}
			}
		}
	}
}
