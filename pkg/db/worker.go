package db

type WorkerJobFetchWalletData struct {
	WalletId      int32   `json:"walletId"`
	WalletAddress string  `json:"walletAddress"`
	Network       Network `json:"network"`
	Fresh         bool    `json:"fresh"`
}

const (
	WorkerJobFetchWallet uint8 = iota
)

const (
	WorkerJobQueued uint8 = iota
	WorkerJobInProgress
	WorkerJobSuccess
	WorkerJobError
	WorkerJobCancelScheduled
	WorkerJobCanceled
)
