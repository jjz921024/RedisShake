package status

import (
	"context"
	"time"

	"RedisShake/internal/config"
	"RedisShake/internal/log"

	"github.com/spf13/viper"
)

type Statusable interface {
	Status() interface{}
	StatusString() string
	StatusConsistent() bool
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

	// run all func in ch
	go func() {
		for f := range ch {
			f()
		}
	}()
}
