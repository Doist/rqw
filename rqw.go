package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Doist/rqw/internal/rqw"

	"github.com/artyom/autoflags"
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)
	config := struct {
		Addr       string        `flag:"redis,redis instance address"`
		Name       string        `flag:"queue,queue name"`
		Program    string        `flag:"worker,path to worker program"`
		Thresh     int           `flag:"threshold,min queue size to spawn workers"`
		Limit      int           `flag:"max,max number of workers"`
		TermSignal int           `flag:"signal,number to send to terminate the worker"`
		Delay      time.Duration `flag:"delay,delay between checks (min. 1s)"`
		Grace      time.Duration `flag:"grace,grace period to keep last worker alive"`
		KillWait   time.Duration `flag:"wait,how much to wait for a worker to gracefully exit before killing it"`

		Debug bool `flag:"d,prefix output with source code addresses"`
	}{
		Addr:       "localhost:6379",
		Name:       "",
		Program:    "",
		Limit:      10,
		TermSignal: int(syscall.SIGTERM),
		Delay:      15 * time.Second,
		Grace:      10 * time.Minute,
		KillWait:   time.Second,
	}
	autoflags.Define(&config)
	flag.Parse()
	if config.Addr == "" || config.Name == "" || config.Program == "" ||
		config.Thresh < 0 ||
		config.Limit < 1 || config.Delay < time.Second {
		flag.Usage()
		os.Exit(1)
	}
	if config.Debug {
		logger.SetFlags(logger.Flags() | log.Lshortfile)
	}
	troop := rqw.NewTroop(
		config.Addr,
		config.Name,
		config.Program,
		config.Thresh,
		config.Limit,
		logger,
	)
	if config.Grace > 0 {
		troop = rqw.WithGracePeriod(troop, config.Grace)
	}
	troop = rqw.WithKillDelay(troop, config.KillWait)
	troop = rqw.WithTermSignal(troop, syscall.Signal(config.TermSignal))
	go func() {
		sigch := make(chan os.Signal, 1)
		signal.Notify(sigch, syscall.SIGUSR1)
		for range sigch {
			troop.LogStderr()
		}
	}()
	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, os.Interrupt, syscall.SIGTERM)
	go troop.Loop(config.Delay)
	logger.Print(<-sigch)
	troop.Shutdown()
}
