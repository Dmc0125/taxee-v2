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
		// event.PrecedingEvents = append(event.PrecedingEvents, r.eventId)

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
	switch {
	case event.Type < db.EventTypeSwapBr:
		transfer := event.Transfers[0]

		switch transfer.Direction {
		case db.EventTransferInternal:
			fromAccountId := inventoryAccountId{event.Network, transfer.FromAccount, transfer.Token}
			fromRecords := inv.accounts[fromAccountId]
			toAccountId := inventoryAccountId{event.Network, transfer.ToAccount, transfer.Token}
			toRecords := inv.accounts[toAccountId]

			remainingAmount := transfer.Amount
			if event.Type == db.EventTypeCloseAccount {
				for _, b := range fromRecords {
					remainingAmount = remainingAmount.Add(b.amount)
				}
				transfer.Amount = remainingAmount
				transfer.Value = remainingAmount.Mul(transfer.Price)
			}

			for len(fromRecords) > 0 && remainingAmount.GreaterThan(decimal.Zero) {
				r := fromRecords[0]
				acqPrice := r.acqPrice
				// event.PrecedingEvents = append(event.PrecedingEvents, r.eventId)

				var movedAmount decimal.Decimal
				remainingAmount, movedAmount = invSubBalance(
					&fromRecords,
					remainingAmount,
				)

				toRecords = append(toRecords, &inventoryRecord{
					// eventId:  r.eventId,
					amount:   movedAmount,
					acqPrice: acqPrice,
				})
			}

			if remainingAmount.GreaterThan(decimal.Zero) {
				transfer.MissingAmount = remainingAmount

				value := remainingAmount.Mul(transfer.Price)
				transfer.Profit = value
				inv.income = inv.income.Add(value)

				toRecords = append(toRecords, &inventoryRecord{
					amount:   remainingAmount,
					acqPrice: transfer.Price,
				})
			}

			inv.accounts[fromAccountId] = fromRecords
			inv.accounts[toAccountId] = toRecords
		case db.EventTransferIncoming:
			if event.Type == db.EventTypeTransfer {
				inv.income = inv.income.Add(transfer.Value)
				transfer.Profit = transfer.Value
			}

			accountId := inventoryAccountId{event.Network, transfer.ToAccount, transfer.Token}
			inv.accounts[accountId] = append(
				inv.accounts[accountId],
				&inventoryRecord{
					eventId:  event.Id,
					amount:   transfer.Amount,
					acqPrice: transfer.Price,
				},
			)
		case db.EventTransferOutgoing:
			invSubFromAccount(
				inv,
				transfer.FromAccount, transfer.Token,
				event,
				transfer.Amount, transfer.Price, &transfer.Profit,
				event.Type == db.EventTypeTransfer,
				&transfer.MissingAmount,
			)
		default:
			assert.True(false, "invalid direction: %d", transfer.Direction)
		}
	default:
		var incomingIdx int
		for i, t := range event.Transfers {
			if t.Direction == db.EventTransferOutgoing {
				incomingIdx = i + 1
				break
			}
		}

		var first, second []*db.EventTransfer
		switch event.Type {
		case db.EventTypeRemoveLiquidity:
			// incoming
			first = event.Transfers[incomingIdx:]
			// outgoing - LP TOKEN
			second = event.Transfers[:incomingIdx]
			assert.True(len(second) == 1, "multiple LP tokens in remove_liquidity")
		default:
			// outgoing - TOKENS
			first = event.Transfers[:incomingIdx]
			// incoming - LP TOKEN
			second = event.Transfers[incomingIdx:]

			if event.Type == db.EventTypeAddLiquidity {
				assert.True(len(second) == 1, "multiple LP tokens in add_liquidity")
			}
		}

		var tokensValue decimal.Decimal

		process := func(transfers []*db.EventTransfer) {
			for _, t := range transfers {
				switch t.Direction {
				case db.EventTransferIncoming:
					if event.Type == db.EventTypeAddLiquidity && t.Value.IsZero() {
						t.Value = tokensValue
						t.Price = tokensValue.Div(t.Amount)
					} else if event.Type == db.EventTypeRemoveLiquidity {
						tokensValue = tokensValue.Add(t.Value)
					}

					accountId := inventoryAccountId{event.Network, t.ToAccount, t.Token}
					inv.accounts[accountId] = append(inv.accounts[accountId], &inventoryRecord{
						// eventId:  event.Id,
						amount:   t.Amount,
						acqPrice: t.Price,
					})
				case db.EventTransferOutgoing:
					if event.Type == db.EventTypeRemoveLiquidity && t.Value.IsZero() {
						t.Value = tokensValue
						t.Price = tokensValue.Div(t.Amount)
					}

					value := invSubFromAccount(
						inv,
						t.FromAccount, t.Token,
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

		process(first)
		process(second)
	}
}
