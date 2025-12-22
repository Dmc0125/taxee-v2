package main

import "taxee/pkg/db"

type networkGlobals struct {
	explorerUrl        string
	explorerAccountUrl string
	imgUrl             string
}

var networksGlobals = map[db.Network]networkGlobals{
	db.NetworkSolana: {
		explorerUrl:        "https://solscan.io",
		explorerAccountUrl: "https://solscan.io/account",
		imgUrl:             "/static/logo_solana.svg",
	},
	db.NetworkArbitrum: {
		explorerUrl:        "https://arbiscan.io",
		explorerAccountUrl: "https://arbiscan.io/address",
		imgUrl:             "/static/logo_arbitrum.svg",
	},
}

var appImgUrls = map[string]string{
	"0-native":           "/static/logo_solana.svg",
	"0-system":           "/static/logo_solana.svg",
	"0-token":            "/static/logo_solana.svg",
	"0-associated_token": "/static/logo_solana.svg",
	"0-meteora_pools":    "/static/logo_meteora.svg",
	"0-meteora_farms":    "/static/logo_meteora.svg",
	"0-jupiter":          "/static/logo_jupiter.svg",
	"0-jupiter_dca":      "/static/logo_jupiter.svg",
	"0-drift":            "/static/logo_drift.svg",
	"3-native":           "/static/logo_arbitrum.svg",
}
