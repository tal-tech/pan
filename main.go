package main

import (
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	"pan/mq"
	_ "pan/mq/delay"
	_ "pan/mq/kafka"
	_ "pan/mq/nsq"
	_ "pan/mq/rabbitmq"
	_ "pan/mq/rocketmq"

	"github.com/spf13/cast"
	logger "github.com/tal-tech/loggerX"
)

var exit = make(chan string, 1)

func main() {
	logger.InitLogger("")
	defer recovery()
	defer logger.Close()
	go dealSignal()

	pm, err := mq.NewProxyManager()
	if err != nil {
		logger.F("maproxy.main.newProxyManager", err)
		os.Exit(1)
	}
	pm.Run(exit)
}

func dealSignal() {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		exit <- "shutdown"
	}()

}

func recovery() {
	if rec := recover(); rec != nil {
		if err, ok := rec.(error); ok {
			fmt.Printf("PanicRecover Unhandled error: %v\n stack:%v\n", err.Error(), cast.ToString(debug.Stack()))
		} else {
			fmt.Printf("PanicRecover Panic: %v\n stack:%v\n", rec, cast.ToString(debug.Stack()))
		}
		exit <- "panic"
	}
}
