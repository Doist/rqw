// +build linux

package rqw

import "github.com/artyom/meminfo"

// memAvail takes amount of memory required and returns whether this much memory
// is available
func memAvail(i int64) bool {
	if i == 0 {
		return true
	}
	mi, err := meminfo.New()
	if err != nil {
		return true
	}
	return i < mi.FreeTotal()
}
