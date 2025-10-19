package requesttimer

import (
	"sync"
	"time"
)

type DefaultTimer struct {
	lastReqTs time.Time
	timeoutMs int64
	mx        *sync.Mutex
}

func NewDefault(timeoutMs int64) *DefaultTimer {
	return &DefaultTimer{
		lastReqTs: time.Unix(0, 0),
		timeoutMs: timeoutMs,
		mx:        &sync.Mutex{},
	}
}

func (timer *DefaultTimer) Lock() {
	timer.mx.Lock()

	now := time.Now().UnixMilli()
	prev := timer.lastReqTs.UnixMilli()
	nextMin := prev + timer.timeoutMs

	if now < nextMin {
		diff := nextMin - now
		time.Sleep(time.Duration(diff) * time.Millisecond)
	}
}

func (timer *DefaultTimer) Free() {
	timer.lastReqTs = time.Now()
	timer.mx.Unlock()
}
