package rqw // import "github.com/Doist/rqw/internal/rqw"

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/davecheney/loadavg"
	"github.com/mediocregopher/radix.v2/redis"
)

const killDelay = time.Second

// Troop is a group of worker processes consuming items from redis queue
// organized as a sorted set.
type Troop struct {
	stderr atomic.Value // holds []byte of stderr of the last unsuccessful command

	m   sync.RWMutex
	log *log.Logger

	addr string // address of redis instance
	name string // key of sorted set queue
	path string // path to program to run
	thr  int    // queue size threshold to spawn workers

	ctx    context.Context
	cancel context.CancelFunc
	gate   Gate                   // used to limit max.number of processes
	procs  map[*exec.Cmd]struct{} // used to access Cmd if need to kill one

	avgMem  int64 // running average of workers' RSS
	samples int64 // number of samples of memory put into running average

	// keep spare worker for this long after queue has been seen non-empty
	gracePeriod      time.Duration
	lastSeenNonEmpty time.Time // last time when queue has been seen non-empty
}

// WithGracePeriod configures troop to use given grace period when calculating
// whether to reap the last worker. If grace period is non-zero, troop will
// spare last worker if queue has been seen non-empty during the past d.
// WithGracePeriod is expected to be run during Troop configuration.
func WithGracePeriod(t *Troop, d time.Duration) *Troop {
	if d < 0 {
		panic("WithGracePeriod called with negative d")
	}
	t.gracePeriod = d
	return t
}

// touch updates lastSeenNonEmpty attribute to current time
func (t *Troop) touch() {
	now := time.Now()
	t.m.Lock()
	defer t.m.Unlock()
	t.lastSeenNonEmpty = now
}

// NewTroop returns new initialized Troop. No error checking is done here, so
// each argument should be non-nil/non-zero, otherwise it would panic elsewhere.
func NewTroop(addr, name, program string, thr, maxWorkers int, logger *log.Logger) *Troop {
	ctx, cancel := context.WithCancel(context.Background())
	return &Troop{
		log:    logger,
		addr:   addr,
		name:   name,
		path:   program,
		thr:    thr,
		ctx:    ctx,
		cancel: cancel,
		gate:   NewGate(maxWorkers),
		procs:  make(map[*exec.Cmd]struct{}),
	}
}

// KillProcess kills random running worker process. If no processes are running,
// this function is no-op. Takes one bool argument, when true, it would spare
// process if it is the only one running.
func (t *Troop) KillProcess(spareLast bool) {
	t.m.Lock()
	defer t.m.Unlock()
	if spareLast && len(t.procs) == 1 {
		return // do not kill last process
	}
	if len(t.procs) == 1 && time.Now().Sub(t.lastSeenNonEmpty) < t.gracePeriod {
		return
	}
	var cmd *exec.Cmd
	for cmd = range t.procs {
		// range over map iterates in random order, we need one
		// iteration to grab random element, so break loop here
		break
	}
	if cmd == nil {
		return
	}
	t.log.Printf("terminate %q [%d]", cmd.Path, cmd.Process.Pid)
	cmd.Process.Signal(syscall.SIGTERM)
	time.Sleep(killDelay)
	cmd.Process.Kill()
}

// LogStderr outputs to log prefix+suffix of the stderr from the most recently
// command that returned with non-nil error.
func (t *Troop) LogStderr() {
	val := t.stderr.Load()
	if val == nil {
		return
	}
	t.log.Printf("most recently failed command stderr:\n%s", val.([]byte))
}

// SpawnProcess starts one worker process if there's capacity for it. Out of
// capacity condition is not an error. If Troop is already shut down,
// ErrTroopDone error returned.
func (t *Troop) SpawnProcess() error {
	select {
	case <-t.ctx.Done():
		return ErrTroopDone
	case t.gate <- struct{}{}:
	default:
		// out of capacity
		return nil
	}
	// throttle spawn rate as we close to max capacity
	gLen := len(t.gate) - 1 // subtract one as we're already acquired lock above
	gCap := cap(t.gate)
	if gLen > rand.Intn(gCap) {
		t.log.Printf("spawn throttled due to capacity increase (%d out of %d)",
			gLen, gCap)
		t.gate.Unlock()
		return nil
	}
	if st := stealRatio(); rand.Float64() < st {
		t.log.Printf("spawn throttled due to high CPU steal ratio (%.2f)", st)
		t.gate.Unlock()
		return nil
	}
	// throttle spawn rate as load average increases
	if la, err := loadavg.LoadAvg(); err == nil &&
		float64(la[1]) > rand.NormFloat64()*3+7 {
		t.log.Printf("spawn throttled due to load average (%.2f)", la[1])
		t.gate.Unlock()
		return nil
	}
	if m := t.rssAverage(); !memAvail(m) {
		t.log.Printf("spawn throttled due to low memory conditions, "+
			"avg. worker RSS is %s", ByteSize(m))
		t.gate.Unlock()
		return nil
	}
	cmd := exec.Command(t.path)
	cmd.Stderr = &prefixSuffixSaver{N: 32 << 10}
	cmd.SysProcAttr = sysProcAttr()
	if err := cmd.Start(); err != nil {
		t.gate.Unlock()
		return err
	}
	t.m.Lock()
	defer t.m.Unlock()
	t.procs[cmd] = struct{}{}
	go func() {
		defer t.gate.Unlock()
		switch err := cmd.Wait(); {
		case err == nil:
			t.log.Printf("process %q [%d] finished (%s)", cmd.Path, cmd.Process.Pid,
				processStats(cmd.ProcessState))
		default:
			t.log.Printf("process %q [%d]: %s (%s)", cmd.Path, cmd.Process.Pid,
				exitReason(err), processStats(cmd.ProcessState))
			if s, ok := cmd.Stderr.(*prefixSuffixSaver); ok && s.prefix != nil {
				t.stderr.Store(s.Bytes())
			}
		}
		t.m.Lock()
		t.updateMemAvg(maxRSS(cmd.ProcessState))
		delete(t.procs, cmd)
		t.m.Unlock()
	}()
	t.log.Printf("process %q [%d] spawned", cmd.Path, cmd.Process.Pid)
	return nil
}

// Shutdown marks Troop as done and kills all worker processes. Loop shuts
// itself down after Shutdown was called.
func (t *Troop) Shutdown() {
	t.cancel()
	t.m.Lock()
	defer t.m.Unlock()
	var wg sync.WaitGroup
	for cmd := range t.procs {
		wg.Add(1)
		go func(cmd *exec.Cmd) {
			defer wg.Done()
			cmd.Process.Signal(syscall.SIGTERM)
			time.Sleep(killDelay)
			cmd.Process.Kill()
		}(cmd)
	}
	wg.Wait()
	t.log.Print("Troop shut down")
}

// done is a convenient method to check whether Troop was already shut down
func (t *Troop) done() bool {
	select {
	case <-t.ctx.Done():
		return true
	default:
		return false
	}
}

// Loop connects to redis instance and polls queue length with ZCOUNT command,
// using 0 as min and current unix timestamp as max values. If queue is has more
// than t.thr elements, Loop spawns extra worker by SpawnProcess call. If queue
// length drops by more than 5% since previous check, Loop kills one worker.
func (t *Troop) Loop(checkDelay time.Duration) {
	defer t.log.Printf("check loop for %q at %q finished", t.name, t.addr)
	retryDelay := 1 * time.Second
	if checkDelay < time.Second {
		checkDelay = time.Second
	}
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
			t.log.Printf("failed to connect to redis at %q: %v", t.addr, err)
			time.Sleep(retryDelay)
			continue CONNLOOP
		}
		prevCnt = -1 // so we won't mix it with empty queue
		for {
			select {
			case <-t.ctx.Done():
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
			if cnt != prevCnt {
				t.log.Printf("%d items in queue", cnt)
			}
			if cnt > 0 {
				t.touch()
			}
			switch {
			case cnt > int64(t.thr):
				t.SpawnProcess()
			case cnt < prevCnt && prevCnt-cnt > (prevCnt/100*5):
				t.KillProcess(true)
			case cnt <= int64(t.thr*2/3):
				t.KillProcess(false)
			}
			prevCnt = cnt
		}
	}
}

// rssAverage returns running average of workers' RSS
func (t *Troop) rssAverage() int64 {
	t.m.RLock()
	n := t.avgMem
	t.m.RUnlock()
	return n
}

// updateMemAvg updates running average of workers' RSS. It is assumed that this
// function is called with t.m lock already held — no locking is done inside
// function.
func (t *Troop) updateMemAvg(rss int64) {
	if rss <= 0 {
		return
	}
	t.avgMem = (t.samples/(t.samples+1))*t.avgMem + rss/(t.samples+1)
	t.samples++
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

func init() { rand.Seed(time.Now().UnixNano()) }
