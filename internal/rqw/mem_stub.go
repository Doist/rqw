// +build !linux

package rqw

// memAvail takes amount of memory required and returns whether this much memory
// is available
func memAvail(i int64) bool { return true }
