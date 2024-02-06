package main

import (
	"context"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"RedisShake/internal/config"
	"RedisShake/internal/log"
	"RedisShake/internal/server"
	"RedisShake/internal/status"
	"RedisShake/internal/utils"

	"github.com/mcuadros/go-defaults"
)

var httpSvr *http.Server

func main() {
	v := config.LoadConfig()

	log.Init(config.Opt.Advanced.LogLevel, config.Opt.Advanced.LogFile, config.Opt.Advanced.Dir)
	utils.ChdirAndAcquireFileLock()
	utils.SetNcpu()
	status.Init()

	ctx, cancel := context.WithCancel(context.Background())

	if v.IsSet("http_server") {
		opts := new(server.HttpServerOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("http_server", opts)
		if err != nil {
			log.Panicf("failed to read the HttpServer config entry. err: %v", err)
		}
		httpSvr = server.StartHttpServer(opts)

	} else {
		_, err := server.CreateAndStartTask(ctx, v)
		if err != nil {
			log.Panicf("start err:%s\n", err.Error())
		}
	}

	waitShutdown(cancel)
}

func waitShutdown(cancel context.CancelFunc) {
	quitCh := make(chan os.Signal, 1)
	signal.Notify(quitCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	sig := <-quitCh
	log.Infof("Got signal: %s to exit.", sig)
	if httpSvr != nil {
		_ = httpSvr.Close()
	}
	cancel()
	utils.ReleaseFileLock() // Release file lock
	log.Infof("all done")
}