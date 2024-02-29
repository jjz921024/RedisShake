package status

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"RedisShake/internal/config"
	"RedisShake/internal/log"

	"github.com/spf13/viper"
)

type Statusable interface {
	Status() interface{}
	StatusString() string
	StatusConsistent() bool
	IdOffset() (string, int64)
}

type Stat struct {
	Time       string `json:"start_time"`
	Consistent bool   `json:"consistent"`
	// function
	TotalEntriesCount  EntryCount            `json:"total_entries_count"`
	PerCmdEntriesCount map[string]EntryCount `json:"per_cmd_entries_count"`
	// reader
	Reader interface{} `json:"reader"`
	// writer
	Writer interface{} `json:"writer"`
}

var ch = make(chan func(), 1000)

type SyncTask struct {
	Ctx    context.Context
	Cancel context.CancelFunc

	ID string
	V  *viper.Viper

	Writer Statusable
	Reader Statusable

	Stat *Stat
}

// TODO: lock
var CurrentTask *SyncTask

func AddReadCount(cmd string) {
	ch <- func() {
		if CurrentTask == nil {
			return
		}
		stat := CurrentTask.Stat
		if stat.PerCmdEntriesCount == nil {
			stat.PerCmdEntriesCount = make(map[string]EntryCount)
		}
		cmdEntryCount, ok := stat.PerCmdEntriesCount[cmd]
		if !ok {
			cmdEntryCount = EntryCount{}
			stat.PerCmdEntriesCount[cmd] = cmdEntryCount
		}
		stat.TotalEntriesCount.ReadCount += 1
		cmdEntryCount.ReadCount += 1
		stat.PerCmdEntriesCount[cmd] = cmdEntryCount
	}
}

func AddWriteCount(cmd string) {
	ch <- func() {
		if CurrentTask == nil {
			return
		}
		stat := CurrentTask.Stat
		if stat.PerCmdEntriesCount == nil {
			stat.PerCmdEntriesCount = make(map[string]EntryCount)
		}
		cmdEntryCount, ok := stat.PerCmdEntriesCount[cmd]
		if !ok {
			cmdEntryCount = EntryCount{}
			stat.PerCmdEntriesCount[cmd] = cmdEntryCount
		}
		stat.TotalEntriesCount.WriteCount += 1
		cmdEntryCount.WriteCount += 1
		stat.PerCmdEntriesCount[cmd] = cmdEntryCount
	}
}

var (
	client = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns: 3,
		},
		Timeout: 5 * time.Second,
	}
)

const (
	hearthCheckPath  = "/worker/status"
	offsetReportPaht = "/msa/task/offset"
)

type hearthInfoBody struct {
	IP   string `json:"ip"`
	Port int    `json:"port"`
	Role string `json:"role"`
}

type syncInfoBody struct {
	TaskId     string `json:"task_id"`
	ReplId     string `json:"repl_id"`
	ReplOffset int64  `json:"repl_offset"`
}

type httpResponse struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

func Init() {
	// for update reader/writer stat
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if CurrentTask == nil {
				continue
			}
			ch <- func() {
				stat := CurrentTask.Stat
				// update reader/writer stat
				stat.Reader = CurrentTask.Reader.Status()
				stat.Writer = CurrentTask.Writer.Status()
				stat.Consistent = CurrentTask.Reader.StatusConsistent() && CurrentTask.Writer.StatusConsistent()

				// update OPS
				stat.TotalEntriesCount.updateOPS()
				for _, cmdEntryCount := range stat.PerCmdEntriesCount {
					cmdEntryCount.updateOPS()
				}
			}
		}
	}()

	// for log to screen
	go func() {
		if config.Opt.Advanced.LogInterval <= 0 {
			log.Infof("log interval is 0, will not log to screen")
			return
		}
		ticker := time.NewTicker(time.Duration(config.Opt.Advanced.LogInterval) * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if CurrentTask == nil {
				continue
			}
			ch <- func() {
				log.Infof("%s, %s", CurrentTask.Stat.TotalEntriesCount.String(), CurrentTask.Reader.StatusString())
			}
		}
	}()

	httpOpt := config.Opt.HttpServer
	if httpOpt.Enable && (httpOpt.Host == "" || httpOpt.AdminUrl == "") {
		log.Panicf("lack of field, host:%s:%d, admin url:%s", httpOpt.Host, httpOpt.HttpPort, httpOpt.AdminUrl)
	}

	// heartbeat to admin
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			ch <- func() {
				body, err := json.Marshal(hearthInfoBody{
					IP:   httpOpt.Host,
					Port: httpOpt.HttpPort,
					Role: "reader",
				})
				if err != nil {
					log.Warnf("json encode err:%s", err.Error())
					return
				}
				req, err := http.NewRequest("POST", httpOpt.AdminUrl+hearthCheckPath, bytes.NewBuffer(body))
				req.Header.Set("Content-Type", "application/json;charset=UTF-8")
				if err != nil {
					log.Warnf("build http req err:%s", err.Error())
					return
				}

				resp, err := client.Do(req)
				if err != nil {
					log.Warnf("heartbeat to admin err:%s", err.Error())
					return
				}
				defer resp.Body.Close()
				dealHttpResponse(req.URL.String(), resp)
			}
		}
	}()

	// report offset
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			if CurrentTask == nil {
				return
			}

			ch <- func() {
				replId, offset := CurrentTask.Reader.IdOffset()
				// 目前只是以节点维度上报
				body, err := json.Marshal(syncInfoBody{
					TaskId:     CurrentTask.ID,
					ReplId:     replId,
					ReplOffset: offset,
				})
				if err != nil {
					log.Warnf("json encode err:%s", err.Error())
					return
				}

				log.Infof("reporte offset to admin taskId:%s, replId:%s, offset:%d", CurrentTask.ID, replId, offset)
				req, err := http.NewRequest("POST", httpOpt.AdminUrl+offsetReportPaht, bytes.NewBuffer(body))
				req.Header.Set("Content-Type", "application/json;charset=UTF-8")
				if err != nil {
					log.Warnf("build http req err:%s", err.Error())
					return
				}

				resp, err := client.Do(req)
				if err != nil {
					log.Warnf("report offset to admin err:%s", err.Error())
					return
				}
				defer resp.Body.Close()
				dealHttpResponse(req.URL.String(), resp)
			}
		}
	}()

	// run all func in ch
	go func() {
		for f := range ch {
			f()
		}
	}()
}

func dealHttpResponse(url string, resp *http.Response) {
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Warnf("read response body fail, url:%s err:%s", url, err.Error())
		return
	} else if resp.StatusCode != 200 {
		log.Warnf("http response status code not expected, url:%s code:%d, content:%s", url, resp.StatusCode, string(content))
		return
	}

	respBody := &httpResponse{}
	err = json.Unmarshal(content, resp)
	if err != nil {
		log.Warnf("parse http response, url:%s, err:%s, resp:%s", url, err.Error(), string(content))
	} else if respBody.Code != 0 {
		log.Warnf("http response not expected, url:%s, code:%d, content%s", url, respBody.Code, string(content))
	}
}
