package main

import (
	"flag"
	"log"
	"os"
	"time"

	"bitbucket.org/artyom_p/rqw/internal/rqw"

	"github.com/artyom/autoflags"
)

func main() {
	config := struct {
		Addr    string        `flag:"redis,redis instance address"`
		Name    string        `flag:"queue,queue name"`
		Program string        `flag:"worker,path to worker program"`
		Limit   int           `flag:"max,max number of workers"`
		Delay   time.Duration `flag:"delay,delay between checks (min. 1s)"`
	}{
		Addr:    "192.168.56.101:6379",
		Name:    "queue",
		Program: "/tmp/worker.sh",
		Limit:   3,
		Delay:   3 * time.Second,
	}
	if err := autoflags.Define(&config); err != nil {
		log.Fatal(err)
	}
	flag.Parse()
	if config.Addr == "" || config.Name == "" || config.Program == "" || config.Limit < 1 || config.Delay < time.Second {
		flag.Usage()
		os.Exit(1)
	}
	troop := rqw.NewTroop(
		config.Addr,
		config.Name,
		config.Program,
		config.Limit,
		log.New(os.Stdout, "", log.Ltime|log.Lshortfile),
	)
	troop.Loop(config.Delay)
}
