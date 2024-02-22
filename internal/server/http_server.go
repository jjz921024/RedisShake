package server

import (
	"RedisShake/internal/log"
	"RedisShake/internal/reader"
	"RedisShake/internal/status"
	"RedisShake/internal/writer"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/spf13/viper"
)

type HttpServerOptions struct {
	HttpPort int `mapstructure:"http_port" default:"8080"`
}

func StartHttpServer(opts *HttpServerOptions) *http.Server {
	mux := mux.NewRouter()
	mux.HandleFunc("/msa/task/start", submitTask)
	mux.HandleFunc("/msa/task/cancel/{id}", cancelTask)
	mux.HandleFunc("/msa/task/info/{id}", displayTask)
	mux.HandleFunc("/msa/task/health/{id}", healthCheck)

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

var (
	successResp httpResponse = httpResponse{0, "success"}
	noTaskResp httpResponse = httpResponse{-1, "not task"}
)

func submitTask(w http.ResponseWriter, r *http.Request) {
	header := w.Header()
	header.Add("content-type", "application/json")

	log.Infof("submit a task from:%s", r.RemoteAddr)

	// 只能运行一个任务
	if status.CurrentTask != nil {
		resp, _ := json.Marshal(httpResponse{-1, []byte("exist running task:" + status.CurrentTask.ID)})
		_, _ = w.Write(resp)
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
	status.CurrentTask, err = CreateAndStartTask(ctx, v, cancel)
	if err != nil {
		log.Warnf("read task info err:%s", err.Error())
		resp, _ := json.Marshal(httpResponse{-1, err.Error()})
		_, _ = w.Write(resp)
		cancel()
		return
	}

	resp, _ := json.Marshal(successResp)
	if _, err := w.Write(resp); err != nil {
		log.Warnf("respone http err:%s", err.Error())
	}
}

func CreateAndStartTask(ctx context.Context, v *viper.Viper, cancel context.CancelFunc) (*status.SyncTask, error) {
	taskId := v.GetString("advanced.task_id")
	if taskId == "" {
		return nil, errors.New("task id is null")
	}

	theReader, err := reader.CreateReader(v)
	if err != nil {
		return nil, err
	}

	theWriter, err := writer.CreateWriter(v)
	if err != nil {
		// TODO: reader释放不了
		return nil, err
	}

	task := &status.SyncTask{
		Ctx:    ctx,
		V:      v,
		ID:     taskId,
		Writer: theWriter,
		Reader: theReader,
		Cancel: cancel,
		Stat:   new(status.Stat),
	}

	task.Stat.Time = time.Now().Format("2006-01-02 15:04:05")
	log.Infof("task:[%s] start syncing...", taskId)

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

	vars := mux.Vars(r)
	taskId := vars["id"]
	log.Infof("request cancel task:%s from:%s", taskId, r.RemoteAddr)

	if status.CurrentTask == nil || status.CurrentTask.ID != taskId {
		log.Infof("current not exist running task")
		resp, _ := json.Marshal(noTaskResp)
		_, _ = w.Write(resp)
		return
	}

	// cancel后会将reader相关的资源释放掉
	status.CurrentTask.Cancel()
	// 关闭writer相关的资源
	if theWriter, ok := status.CurrentTask.Writer.(writer.Writer); ok {
		theWriter.Close()
	}
	status.CurrentTask = nil
	log.Infof("cancel running task success")

	resp, _ := json.Marshal(successResp)
	if _, err := w.Write(resp); err != nil {
		log.Warnf("respone http err:%s", err.Error())
	}
}

// 列出正在运行的任务信息
func displayTask(w http.ResponseWriter, r *http.Request) {
	header := w.Header()
	header.Add("content-type", "application/json")

	vars := mux.Vars(r)
	taskId := vars["id"]
	log.Infof("display task:%s from:%s", taskId, r.RemoteAddr)

	if status.CurrentTask == nil || status.CurrentTask.ID != taskId {
		resp, _ := json.Marshal(noTaskResp)
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

// 任务健康检查
func healthCheck(w http.ResponseWriter, r *http.Request) {
	header := w.Header()
	header.Add("content-type", "application/json")

	vars := mux.Vars(r)
	taskId := vars["id"]
	log.Infof("check task:%s from:%s", taskId, r.RemoteAddr)

	if status.CurrentTask == nil || status.CurrentTask.ID != taskId {
		resp, _ := json.Marshal(noTaskResp)
		_, _ = w.Write(resp)
		return
	}

	resp, _ := json.Marshal(successResp)
	if _, err := w.Write(resp); err != nil {
		log.Warnf("respone http err:%s", err.Error())
	}
}