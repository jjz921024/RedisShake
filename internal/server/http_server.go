package server

import (
	"RedisShake/internal/config"
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
	HttpPort int `mapstructure:"http_port" default:"8080"`
}

func StartHttpServer(opts *HttpServerOptions) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/submit", submitTask)
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
			log.Infof("http server err:%s", err.Error())
		}
	}()

	log.Infof("http server listen on:%d", opts.HttpPort)
	return httpSvr
}

type httpResponse struct {
	Code    int         `json:"code"`
	Message interface{} `json:"message,omitempty"`
}

var successHttpResponse httpResponse = httpResponse{0, "success"}

func submitTask(w http.ResponseWriter, r *http.Request) {
	header := w.Header()
	header.Add("content-type", "application/json")

	log.Infof("submit a task from:%s", r.RemoteAddr)

	// 只能运行一个任务
	if status.CurrentTask != nil {
		if _, err := w.Write([]byte("exist running task")); err != nil {
			log.Warnf("respone http err:%s", err.Error())
		}
		return
	}

	v := viper.New()
	v.SetConfigType("json")
	if err := v.ReadConfig(r.Body); err != nil {
		log.Warnf("read task info err:%s", err.Error())
		resp, _ := json.Marshal(httpResponse{-1, err.Error()})
		_, _ = w.Write(resp)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())

	// 创建并运行任务
	var err error
	status.CurrentTask, err = CreateAndStartTask(ctx, v)
	if err != nil {
		log.Warnf("read task info err:%s", err.Error())
		resp, _ := json.Marshal(httpResponse{-1, err.Error()})
		_, _ = w.Write(resp)
		cancel()
		return
	}
	status.CurrentTask.Cancel = cancel

	resp, _ := json.Marshal(successHttpResponse)
	if _, err := w.Write(resp); err != nil {
		log.Warnf("respone http err:%s", err.Error())
	}
}

func CreateAndStartTask(ctx context.Context, v *viper.Viper) (*status.SyncTask, error) {
	theReader, err := reader.CreateReader(v)
	if err != nil {
		return nil, err
	}

	theWriter, err := writer.CreateWriter(v, config.Opt.Advanced)
	if err != nil {
		return nil, err
	}

	task := &status.SyncTask{
		Ctx:    ctx,
		V:      v,
		Writer: theWriter,
		Reader: theReader,
		Stat:   new(status.Stat),
	}

	task.Stat.Time = time.Now().Format("2006-01-02 15:04:05")
	log.Infof("start syncing...")

	ch := theReader.StartRead(ctx)
	go func() {
		for e := range ch {
			// calc arguments
			e.Parse()
			status.AddReadCount(e.CmdName)

			theWriter.Write(e)
			status.AddWriteCount(e.CmdName)
		}
	}()

	return task, nil
}

// 取消正在运行的任务
func cancelTask(w http.ResponseWriter, r *http.Request) {
	header := w.Header()
	header.Add("content-type", "application/json")

	log.Infof("request cancel running task from:%s", r.RemoteAddr)

	if status.CurrentTask == nil {
		log.Infof("current not exist running task")
		resp, _ := json.Marshal(httpResponse{0, "no task"})
		_, _ = w.Write(resp)
		return
	}

	status.CurrentTask.Cancel()
	if theWriter, ok := status.CurrentTask.Writer.(writer.Writer); ok {
		theWriter.Close()
	}
	status.CurrentTask = nil
	log.Infof("cancel running task success")

	resp, _ := json.Marshal(successHttpResponse)
	if _, err := w.Write(resp); err != nil {
		log.Warnf("respone http err:%s", err.Error())
	}
}

// 列出正在运行的任务信息
func displayTask(w http.ResponseWriter, r *http.Request) {
	header := w.Header()
	header.Add("content-type", "application/json")

	log.Infof("display running task from:%s", r.RemoteAddr)

	if status.CurrentTask == nil {
		resp, _ := json.Marshal(httpResponse{0, "no task"})
		_, _ = w.Write(resp)
		return
	}

	settings := status.CurrentTask.V.AllSettings()
	//s := status.CurrentTask.Stat
	response := httpResponse{
		Code:    0,
		Message: settings,
	}

	resp, _ := json.Marshal(response)
	if _, err := w.Write(resp); err != nil {
		log.Warnf("respone http err:%s", err.Error())
	}
}
