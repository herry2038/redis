package redis

type ExecMODE int

const (
	MODE_DEFAULT     ExecMODE = 0
	MODE_CONSISTENCY ExecMODE = 1
	MODE_IDC_RWS     ExecMODE = 2
	MODE_RAMDOM_RWS  ExecMODE = 3
	MODE_LATENCY_RWS ExecMODE = 4
)
