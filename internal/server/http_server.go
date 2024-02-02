package server

import (
	"RedisShake/internal/config"
	"RedisShake/internal/function"
	"RedisShake/internal/log"
	"RedisShake/internal/reader"
	"RedisShake/internal/status"
	"RedisShake/internal/writer"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/spf13/viper"
)

type HttpServerOptions struct {
	HttpPort     int `mapstructure:"http_port" default:"8080"`
}

func StartHttpServer(opts *HttpServerOptions) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/sumbit", submitTask)
	mux.HandleFunc("/cancel", cancelTask)
	mux.HandleFunc("/display", displayTask)

	httpSvr := &http.Server{
		Addr:         ":" + strconv.Itoa(opts.HttpPort),
		WriteTimeout: time.Second * 5,
		Handler:      mux,
	}

	go func() {
		err := httpSvr.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Infof("http server err:%s\n", err.Error())
		}
	}()

	log.Infof("http server listen on:%d\n", opts.HttpPort)
	return httpSvr
}

type syncTask struct {
	ctx context.Context
	cancel context.CancelFunc
	v *viper.Viper
	writer writer.Writer
}

var currentTask *syncTask

type httpResponse struct {
	Code    int    `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

var successHttpResponse httpResponse = httpResponse{0, "success"}


func submitTask(w http.ResponseWriter, r *http.Request) {
	header := w.Header()
	header.Add("content-type", "application/json")

	log.Infof("submit a task from:%s\n", r.RemoteAddr)

	// 只能运行一个任务
	if currentTask != nil {
		if _, err := w.Write([]byte("exist running task")); err != nil {
			log.Warnf("respone http err:%s\n", err.Error())
		}
		return
	}

	v := viper.New()
	v.SetConfigType("json")
	if err := v.ReadConfig(r.Body); err != nil {
		log.Warnf("read task info err:%s\n", err.Error())
		resp, _ := json.Marshal(httpResponse{-1, err.Error()})
		_, _ = w.Write(resp)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())

	// 创建并运行任务
	if currentTask, err := CreateAndStartTask(ctx, v); err != nil {
		log.Warnf("read task info err:%s\n", err.Error())
		resp, _ := json.Marshal(httpResponse{-1, err.Error()})
		_, _ = w.Write(resp)
		cancel()
		return
	} else {
		currentTask.cancel = cancel
	}

	resp, _ := json.Marshal(successHttpResponse)
	if _, err := w.Write(resp); err != nil {
		log.Warnf("respone http err:%s\n", err.Error())
	}
}

// TODO: viper
func CreateAndStartTask(ctx context.Context, v *viper.Viper) (*syncTask, error) {
	theReader, err := reader.CreateReader(v)
	if err != nil {
		return nil, err
	}

	theWriter, err := writer.CreateWriter(v, config.Opt.Advanced)
	if err != nil {
		return nil, err
	}

	status.Init(theReader, theWriter)
	log.Infof("start syncing...")

	ch := theReader.StartRead(ctx)
	go func() {
		for e := range ch {
			// calc arguments
			e.Parse()
			status.AddReadCount(e.CmdName)

			// filter
			log.Debugf("function before: %v", e)
			entries := function.RunFunction(e)
			log.Debugf("function after: %v", entries)

			for _, entry := range entries {
				entry.Parse()
				theWriter.Write(entry)
				status.AddWriteCount(entry.CmdName)
			}
		}
	}()

	return &syncTask{
		ctx: ctx,
		v: v,
		writer: theWriter,
	}, nil
}

// 取消正在运行的任务
func cancelTask(w http.ResponseWriter, r *http.Request) {
	header := w.Header()
	header.Add("content-type", "application/json")

	log.Infof("cancel running task from:%s\n", r.RemoteAddr)

	if currentTask == nil {
		log.Infof("current not exist running task")
		resp, _ := json.Marshal(httpResponse{0, "no task"})
		_, _= w.Write(resp)
		return
	}

	log.Infof("cancel running task")
	currentTask.cancel()
	currentTask.writer.Close()
	currentTask = nil

	resp, _ := json.Marshal(successHttpResponse)
	if _, err := w.Write(resp); err != nil {
		log.Warnf("respone http err:%s\n", err.Error())
	}
}

// 列出正在运行的任务信息
func displayTask(w http.ResponseWriter, r *http.Request) {
	header := w.Header()
	header.Add("content-type", "application/json")

	log.Infof("display running task from:%s\n", r.RemoteAddr)

	if currentTask == nil {
		resp, _ := json.Marshal(httpResponse{0, "no task"})
		_, _= w.Write(resp)
		return
	}

	/* settings, err := json.Marshal(currentTask.v.AllSettings())
	if err != nil {
		log.Infof("marshal config err:%s\n", err.Error())
		_, _= w.Write([]byte("config err"))
		return
	} */

	response := httpResponse{
		Code: 0,
		Message: "",
	}

	resp, _ := json.Marshal(response)
	if _, err := w.Write(resp); err != nil {
		log.Warnf("respone http err:%s\n", err.Error())
	}
}