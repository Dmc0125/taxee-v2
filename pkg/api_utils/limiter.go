package apiutils

import (
	"sync"
	"time"
)

type Limiter struct {
	lastReqTs time.Time
	timeoutMs int64
	mx        *sync.Mutex
}

func NewLimiter(timeoutMs int64) *Limiter {
	return &Limiter{
		lastReqTs: time.Unix(0, 0),
		timeoutMs: timeoutMs,
		mx:        &sync.Mutex{},
	}
}

func (timer *Limiter) Lock() {
	timer.mx.Lock()

	now := time.Now().UnixMilli()
	prev := timer.lastReqTs.UnixMilli()
	nextMin := prev + timer.timeoutMs

	if now < nextMin {
		diff := nextMin - now
		time.Sleep(time.Duration(diff) * time.Millisecond)
	}
}

func (timer *Limiter) Free() {
	timer.lastReqTs = time.Now()
	timer.mx.Unlock()
}
