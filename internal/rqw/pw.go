package rqw

import (
	"errors"
	"fmt"
	"log"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/fzzy/radix/redis"
)

// Troop is a group of worker processes consuming items from redis queue
// organized as a sorted set.
type Troop struct {
	m   sync.RWMutex
	log *log.Logger

	addr string // address of redis instance
	name string // key of sorted set queue
	path string // path to program to run

	quit  chan struct{}          // used to signal loop to quit
	gate  Gate                   // used to limit max.number of processes
	procs map[*exec.Cmd]struct{} // used to access Cmd if need to kill one
}

// NewTroop returns new initialized Troop. No error checking is done here, so
// each argument should be non-nil/non-zero, otherwise it would panic elsewhere.
func NewTroop(addr, name, program string, maxWorkers int, logger *log.Logger) *Troop {
	return &Troop{
		log:   logger,
		addr:  addr,
		name:  name,
		path:  program,
		gate:  NewGate(maxWorkers),
		procs: make(map[*exec.Cmd]struct{}),
	}
}

// KillProcess kills random running worker process. If no processes are running,
// this function is no-op.
func (t *Troop) KillProcess() error {
	t.m.Lock()
	defer t.m.Unlock()
	for cmd := range t.procs {
		if cmd.Process != nil {
			t.log.Printf("kill process %q", cmd.Path)
			// XXX: is TERM better here?
			// return cmd.Process.Signal(syscall.SIGTERM)
			return cmd.Process.Kill()
		}
	}
	return nil
}

// SpawnProcess starts one worker process if there's capacity for it. Out of
// capacity condition is not an error. If Troop is already shut down,
// ErrTroopDone error returned.
func (t *Troop) SpawnProcess() error {
	select {
	case <-t.quit:
		return ErrTroopDone
	case t.gate <- struct{}{}:
	default:
		// out of capacity
		return nil
	}
	cmd := exec.Command(t.path)
	// TODO: add SysProcAttr, see github.com/artyom/tentacles
	// cmd.SysProcAttr = sysProcAttr()
	if err := cmd.Start(); err != nil {
		t.gate.Unlock()
		return err
	}
	t.m.Lock()
	defer t.m.Unlock()
	t.procs[cmd] = struct{}{}
	go func() {
		defer t.gate.Unlock()
		if err := cmd.Wait(); err != nil {
			t.log.Printf("process %q: %s", cmd.Path, exitReason(err))
		}
		t.m.Lock()
		defer t.m.Unlock()
		delete(t.procs, cmd)
	}()
	t.log.Printf("process %q spawned", cmd.Path)
	return nil
}

// Shutdown marks Troop as done and kills all worker processes. Loop shuts
// itself down after Shutdown was called.
func (t *Troop) Shutdown() {
	select {
	case <-t.quit: // already closed
	default:
		close(t.quit)
	}
	t.m.Lock()
	defer t.m.Unlock()
	for cmd := range t.procs {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
	}
	t.log.Print("Troop shut down")
}

// done is a convenient method to check whether Troop was already shut down
func (t *Troop) done() bool {
	select {
	case <-t.quit:
		return true
	default:
		return false
	}
}

// Loop connects to redis instance and polls queue length with ZCOUNT command,
// using 0 as min and current unix timestamp as max values. If queue length
// increased from previous run or stayed at the same positive count, Loop spawns
// extra worker by SpawnProcess call.  If queue length drops by more than 5%
// since previous check, Loop kills one worker.
func (t *Troop) Loop(checkDelay time.Duration) {
	defer t.log.Printf("check loop for %q at %q finished", t.name, t.addr)
	retryDelay := 1 * time.Second
	ticker := time.NewTicker(checkDelay)
	defer ticker.Stop()
	var prevCnt int64 // queue length from previous run
CONNLOOP:
	for {
		if t.done() {
			return
		}
		client, err := redis.DialTimeout("tcp", t.addr, 10*time.Second)
		if err != nil {
			t.log.Printf("failed to connect to redis at %q: ", t.addr, err)
			time.Sleep(retryDelay)
			continue CONNLOOP
		}
		prevCnt = -1 // so we won't mix it with empty queue
		for {
			select {
			case <-t.quit:
				client.Close()
				return
			case <-ticker.C:
			}
			resp := client.Cmd("ZCOUNT", t.name, 0, time.Now().Unix())
			cnt, err := resp.Int64()
			if err != nil {
				t.log.Print("failed to get ZCOUNT: ", err)
				client.Close()
				continue CONNLOOP
			}
			switch {
			case cnt < 0:
				continue
			case cnt > 0 && cnt >= prevCnt:
				t.SpawnProcess()
			case cnt < prevCnt && prevCnt-cnt > (prevCnt/100*5):
				t.KillProcess() // TODO: we can kill the last one worker even if queue still not empty
			case cnt == 0:
				t.KillProcess()
			}
			prevCnt = cnt
		}
	}
}

// exitReason translates error returned by os.Process.Wait() into human-readable
// form.
func exitReason(err error) string {
	exiterr, ok := err.(*exec.ExitError)
	if !ok {
		return err.Error()
	}
	status := exiterr.Sys().(syscall.WaitStatus)
	switch {
	case status.Exited():
		return fmt.Sprintf("exit code %d", status.ExitStatus())
	case status.Signaled():
		return fmt.Sprintf("exit code %d (%s)",
			128+int(status.Signal()), status.Signal())
	}
	return err.Error()
}

var (
	ErrTroopDone = errors.New("troop already quit")
)

// Gate used to limit number of concurrent workers. Gate can be used as
// sync.Locker, but also can be used directly as a channel in a non-blocking
// way:
//
// 	select {
//	case <-gate:
//		// lock acquired successfully
//	default:
//		// failed to acquire lock
// 	}
type Gate chan struct{}

func (g Gate) Lock()   { g <- struct{}{} }
func (g Gate) Unlock() { <-g }

// NewGate returns new initialized Gate with given capacity. Gate of size
// 1 works like sync.Mutex.
func NewGate(size int) Gate {
	if size < 1 {
		size = 1
	}
	return make(chan struct{}, size)
}