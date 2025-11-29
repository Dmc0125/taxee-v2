package parser

import (
	"taxee/pkg/assert"
	"taxee/pkg/db"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type inventoryAccountId struct {
	network db.Network
	address string
	token   string
}

type inventoryRecord struct {
	eventId  uuid.UUID
	amount   decimal.Decimal
	acqPrice decimal.Decimal
}

type inventory struct {
	acquisition decimal.Decimal
	disposal    decimal.Decimal
	income      decimal.Decimal
	accounts    map[inventoryAccountId][]*inventoryRecord
}

func invSubBalance(
	balances *[]*inventoryRecord,
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
	event *db.Event,
	amount, price decimal.Decimal,
	profit *decimal.Decimal,
	increaseProfits bool,
	missingAmount *decimal.Decimal,
) decimal.Decimal {
	accountId := inventoryAccountId{event.Network, account, token}
	records := inv.accounts[accountId]

	remainingAmount := amount
	value := decimal.Zero

	for len(records) > 0 && remainingAmount.GreaterThan(decimal.Zero) {
		r := records[0]
		acqPrice := r.acqPrice
		event.PrecedingEvents = append(event.PrecedingEvents, r.eventId)

		var movedAmount decimal.Decimal
		remainingAmount, movedAmount = invSubBalance(
			&records,
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
		*missingAmount = remainingAmount

		if increaseProfits {
			remainingValue := remainingAmount.Mul(price)
			*profit = remainingValue
			inv.income = inv.income.Add(remainingValue)

			value = value.Add(remainingValue)
		}
	}

	inv.accounts[accountId] = records

	return value
}

func (inv *inventory) processEvent(event *db.Event) {
	switch data := event.Data.(type) {
	case *db.EventTransfer:
		switch data.Direction {
		case db.EventTransferInternal:
			fromAccountId := inventoryAccountId{event.Network, data.FromAccount, data.Token}
			fromRecords := inv.accounts[fromAccountId]
			toAccountId := inventoryAccountId{event.Network, data.ToAccount, data.Token}
			toRecords := inv.accounts[toAccountId]

			remainingAmount := data.Amount
			if event.Type == db.EventTypeCloseAccount {
				for _, b := range fromRecords {
					remainingAmount = remainingAmount.Add(b.amount)
				}
				data.Amount = remainingAmount
				data.Value = remainingAmount.Mul(data.Price)
			}

			for len(fromRecords) > 0 && remainingAmount.GreaterThan(decimal.Zero) {
				r := fromRecords[0]
				acqPrice := r.acqPrice
				event.PrecedingEvents = append(event.PrecedingEvents, r.eventId)

				var movedAmount decimal.Decimal
				remainingAmount, movedAmount = invSubBalance(
					&fromRecords,
					remainingAmount,
				)

				toRecords = append(toRecords, &inventoryRecord{
					eventId:  r.eventId,
					amount:   movedAmount,
					acqPrice: acqPrice,
				})
			}

			if remainingAmount.GreaterThan(decimal.Zero) {
				data.MissingAmount = remainingAmount

				value := remainingAmount.Mul(data.Price)
				data.Profit = value
				inv.income = inv.income.Add(value)

				toRecords = append(toRecords, &inventoryRecord{
					amount:   remainingAmount,
					acqPrice: data.Price,
				})
			}

			inv.accounts[fromAccountId] = fromRecords
			inv.accounts[toAccountId] = toRecords
		case db.EventTransferIncoming:
			if event.Type == db.EventTypeTransfer {
				inv.income = inv.income.Add(data.Value)
				data.Profit = data.Value
			}

			accountId := inventoryAccountId{event.Network, data.ToAccount, data.Token}
			inv.accounts[accountId] = append(
				inv.accounts[accountId],
				&inventoryRecord{
					eventId:  event.Id,
					amount:   data.Amount,
					acqPrice: data.Price,
				},
			)
		case db.EventTransferOutgoing:
			invSubFromAccount(
				inv,
				data.FromAccount, data.Token,
				event,
				data.Amount, data.Price, &data.Profit,
				event.Type == db.EventTypeTransfer,
				&data.MissingAmount,
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

		processItem := func(item queued) {
			if item.incoming {
				for _, t := range item.transfers {
					if event.Type == db.EventTypeAddLiquidity && t.Value.IsZero() {
						t.Value = tokensValue
						t.Price = tokensValue.Div(t.Amount)
					} else if event.Type == db.EventTypeRemoveLiquidity {
						tokensValue = tokensValue.Add(t.Value)
					}

					accountId := inventoryAccountId{event.Network, t.Account, t.Token}
					inv.accounts[accountId] = append(inv.accounts[accountId], &inventoryRecord{
						eventId:  event.Id,
						amount:   t.Amount,
						acqPrice: t.Price,
					})
				}
			} else {
				for _, t := range item.transfers {
					if event.Type == db.EventTypeRemoveLiquidity && t.Value.IsZero() {
						t.Value = tokensValue
						t.Price = tokensValue.Div(t.Amount)
					}

					value := invSubFromAccount(
						inv,
						t.Account, t.Token,
						event,
						t.Amount, t.Price, &t.Profit,
						true,
						&t.MissingAmount,
					)
					if event.Type == db.EventTypeAddLiquidity {
						tokensValue = tokensValue.Add(value)
					}
				}
			}
		}

		processItem(queue[0])
		processItem(queue[1])
	}
}
