package writer

import (
	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"RedisShake/internal/status"

	"github.com/mcuadros/go-defaults"
	"github.com/spf13/viper"
)

type Writer interface {
	status.Statusable
	Write(entry *entry.Entry)
	Close()
}

func CreateWriter(v *viper.Viper, cfg config.AdvancedOptions) (Writer, error) {
	var theWriter Writer
	if v.IsSet("redis_writer") {
		opts := new(RedisWriterOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("redis_writer", opts)
		if err != nil {
			log.Panicf("failed to read the RedisStandaloneWriter config entry. err: %v", err)
		}
		if opts.OffReply && cfg.RDBRestoreCommandBehavior == "panic" {
			log.Panicf("the RDBRestoreCommandBehavior can't be 'panic' when the server not reply to commands")
		}
		if opts.Cluster {
			theWriter = NewRedisClusterWriter(opts)
			log.Infof("create RedisClusterWriter: %v", opts.Address)
		} else {
			theWriter = NewRedisStandaloneWriter(opts)
			log.Infof("create RedisStandaloneWriter: %v", opts.Address)
		}
		if cfg.EmptyDBBeforeSync {
			// exec FLUSHALL command to flush db
			entry := entry.NewEntry()
			entry.Argv = []string{"FLUSHALL"}
			theWriter.Write(entry)
		}
	} else {
		log.Panicf("no writer config entry found")
	}
	return theWriter, nil
}