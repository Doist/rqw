package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bitbucket.org/artyom_p/rqw/internal/rqw"

	"github.com/artyom/autoflags"
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)
	config := struct {
		Addr    string        `flag:"redis,redis instance address"`
		Name    string        `flag:"queue,queue name"`
		Program string        `flag:"worker,path to worker program"`
		Limit   int           `flag:"max,max number of workers"`
		Delay   time.Duration `flag:"delay,delay between checks (min. 1s)"`

		Debug bool `flag:"d,prefix output with source code addresses"`
	}{
		Addr:    "192.168.56.101:6379",
		Name:    "queue",
		Program: "/tmp/worker.sh",
		Limit:   3,
		Delay:   3 * time.Second,
	}
	if err := autoflags.Define(&config); err != nil {
		logger.Fatal(err)
	}
	flag.Parse()
	if config.Addr == "" || config.Name == "" || config.Program == "" ||
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
		config.Limit,
		logger,
	)
	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, os.Interrupt, syscall.SIGTERM)
	go troop.Loop(config.Delay)
	logger.Print(<-sigch)
	troop.Shutdown()
}
