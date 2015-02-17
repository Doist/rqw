// +build linux

package rqw

import (
	"fmt"
	"os"
	"sync"
)

var stealRatio func() float64

func init() {
	stealRatio = new(ticksInfo).stealRatio
}

type ticksInfo struct {
	m     sync.Mutex
	total uint64
	steal uint64
}

func (ti *ticksInfo) stealRatio() float64 {
	f, err := os.Open(`/proc/stat`)
	if err != nil {
		return 0
	}
	defer f.Close()
	var user, nice, system, idle, steal uint64
	var stub uint64
	n, err := fmt.Fscanf(f, "cpu %d %d %d %d %d %d %d %d",
		&user, &nice, &system, &idle,
		&stub, &stub, &stub,
		&steal,
	)
	if err != nil || n != 8 {
		return 0
	}
	ti.m.Lock()
	defer ti.m.Unlock()
	totaldiff := float64(user + nice + system + idle - ti.total)
	if totaldiff == 0 {
		return 0
	}
	out := float64(steal-ti.steal) / totaldiff
	ti.total = user + nice + system + idle
	ti.steal = steal
	return out
}
