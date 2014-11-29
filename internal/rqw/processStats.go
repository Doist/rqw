package rqw

import (
	"fmt"
	"os"
	"syscall"
)

// processStats returns finished process' CPU / memory statistics in
// human-readable form.
func processStats(st *os.ProcessState) string {
	if st == nil {
		return ""
	}
	if r, ok := st.SysUsage().(*syscall.Rusage); ok && r != nil {
		return fmt.Sprintf("sys: %s, user: %s, maxRSS: %s",
			st.SystemTime(),
			st.UserTime(),
			ByteSize(r.Maxrss<<10),
		)
	}
	return fmt.Sprintf("sys: %s, user: %s", st.SystemTime(), st.UserTime())
}

// ByteSize implements Stringer interface for printing size in human-readable
// form
type ByteSize float64

const (
	_           = iota // ignore first value by assigning to blank identifier
	KB ByteSize = 1 << (10 * iota)
	MB
	GB
	TB
	PB
	EB
	ZB
	YB
)

func (b ByteSize) String() string {
	switch {
	case b >= YB:
		return fmt.Sprintf("%.2fYB", b/YB)
	case b >= ZB:
		return fmt.Sprintf("%.2fZB", b/ZB)
	case b >= EB:
		return fmt.Sprintf("%.2fEB", b/EB)
	case b >= PB:
		return fmt.Sprintf("%.2fPB", b/PB)
	case b >= TB:
		return fmt.Sprintf("%.2fTB", b/TB)
	case b >= GB:
		return fmt.Sprintf("%.2fGB", b/GB)
	case b >= MB:
		return fmt.Sprintf("%.2fMB", b/MB)
	case b >= KB:
		return fmt.Sprintf("%.2fKB", b/KB)
	}
	return fmt.Sprintf("%.2fB", b)
}
