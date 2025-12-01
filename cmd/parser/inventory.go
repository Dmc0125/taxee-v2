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
	transferId uuid.UUID
	amount     decimal.Decimal
	acqPrice   decimal.Decimal
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
	event *db.Event,
	transfer *db.EventTransfer,
	increaseProfits bool,
) decimal.Decimal {
	accountId := inventoryAccountId{event.Network, transfer.FromAccount, transfer.Token}
	records := inv.accounts[accountId]

	remainingAmount := transfer.Amount
	value := decimal.Zero

	for len(records) > 0 && remainingAmount.GreaterThan(decimal.Zero) {
		r := records[0]
		acqPrice := r.acqPrice

		var movedAmount decimal.Decimal
		remainingAmount, movedAmount = invSubBalance(
			&records,
			remainingAmount,
		)

		transfer.Sources = append(
			transfer.Sources,
			&db.EventTransferSource{
				TransferId: r.transferId,
				UsedAmount: movedAmount,
			},
		)

		if increaseProfits {
			disposal := movedAmount.Mul(transfer.Price)
			acquisition := movedAmount.Mul(acqPrice)

			transfer.Profit = disposal.Sub(acquisition)
			inv.disposal = inv.disposal.Add(disposal)
			inv.acquisition = inv.acquisition.Add(acquisition)

			value = value.Add(disposal)
		}
	}

	if remainingAmount.GreaterThan(decimal.Zero) {
		transfer.MissingAmount = remainingAmount

		if increaseProfits {
			remainingValue := remainingAmount.Mul(transfer.Price)
			transfer.Profit = transfer.Profit.Add(remainingValue)
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

				var movedAmount decimal.Decimal
				remainingAmount, movedAmount = invSubBalance(
					&fromRecords,
					remainingAmount,
				)

				transfer.Sources = append(
					transfer.Sources,
					&db.EventTransferSource{
						TransferId: r.transferId,
						UsedAmount: movedAmount,
					},
				)

				toRecords = append(toRecords, &inventoryRecord{
					transferId: r.transferId,
					amount:     movedAmount,
					acqPrice:   acqPrice,
				})
			}

			if remainingAmount.GreaterThan(decimal.Zero) {
				transfer.MissingAmount = remainingAmount

				value := remainingAmount.Mul(transfer.Price)
				transfer.Profit = value
				inv.income = inv.income.Add(value)

				toRecords = append(toRecords, &inventoryRecord{
					transferId: transfer.Id,
					amount:     remainingAmount,
					acqPrice:   transfer.Price,
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
					transferId: transfer.Id,
					amount:     transfer.Amount,
					acqPrice:   transfer.Price,
				},
			)
		case db.EventTransferOutgoing:
			increaseProfits := false
			switch event.Type {
			case db.EventTypeTransfer, db.EventTypeCloseAccount:
				increaseProfits = true
			}
			invSubFromAccount(
				inv,
				event,
				transfer,
				increaseProfits,
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
						transferId: t.Id,
						amount:     t.Amount,
						acqPrice:   t.Price,
					})
				case db.EventTransferOutgoing:
					if event.Type == db.EventTypeRemoveLiquidity && t.Value.IsZero() {
						t.Value = tokensValue
						t.Price = tokensValue.Div(t.Amount)
					}

					value := invSubFromAccount(inv, event, t, true)
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
