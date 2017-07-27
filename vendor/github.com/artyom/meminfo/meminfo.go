// +build linux

// meminfo package provides a simple interface to get free system memory by
// parsing `/proc/meminfo`.
package meminfo // import "github.com/artyom/meminfo"

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"sync"
)

var memfile = `/proc/meminfo`

// MemInfo holds data read from `/proc/meminfo`.
type MemInfo struct {
	m       sync.RWMutex
	free    uint32
	buffers uint32
	cached  uint32
}

// New returns new MemInfo populated by values from `/proc/meminfo`.
func New() (*MemInfo, error) {
	mi := new(MemInfo)
	if err := mi.Update(); err != nil {
		return nil, err
	}
	return mi, nil
}

// Update updates MemInfo with data from `/proc/meminfo`.
func (mi *MemInfo) Update() error {
	f, err := os.Open(memfile)
	if err != nil {
		return err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	m := new(MemInfo)
	n := 3 // number of fields we're interested in
	for s.Scan() && n > 0 {
		switch {
		case bytes.HasPrefix(s.Bytes(), []byte(`MemFree:`)):
			_, err = fmt.Sscanf(s.Text(), "MemFree:%d", &m.free)
		case bytes.HasPrefix(s.Bytes(), []byte(`Buffers:`)):
			_, err = fmt.Sscanf(s.Text(), "Buffers:%d", &m.buffers)
		case bytes.HasPrefix(s.Bytes(), []byte(`Cached:`)):
			_, err = fmt.Sscanf(s.Text(), "Cached:%d", &m.cached)
		default:
			continue
		}
		if err != nil {
			return err
		}
		n--
	}
	if err = s.Err(); err != nil {
		return err
	}
	mi.m.Lock()
	defer mi.m.Unlock()
	mi.free = m.free
	mi.buffers = m.buffers
	mi.cached = m.cached
	return nil
}

// FreeTotal returns free memory in bytes with buffers and caches included. This
// value corresponds to `-/+ buffers/cache` row of `free` command.
func (mi *MemInfo) FreeTotal() int64 {
	mi.m.RLock()
	defer mi.m.RUnlock()
	return int64(mi.free+mi.buffers+mi.cached) << 10
}

// Free returns free memory in bytes NOT including buffers and caches. This
// value is reported by `free` command in first line of output.
func (mi *MemInfo) Free() int64 {
	mi.m.RLock()
	defer mi.m.RUnlock()
	return int64(mi.free) << 10
}

// Cached returns number of bytes cached.
func (mi *MemInfo) Cached() int64 {
	mi.m.RLock()
	defer mi.m.RUnlock()
	return int64(mi.cached) << 10
}

// Buffers returns number of bytes in buffers.
func (mi *MemInfo) Buffers() int64 {
	mi.m.RLock()
	defer mi.m.RUnlock()
	return int64(mi.buffers) << 10
}
